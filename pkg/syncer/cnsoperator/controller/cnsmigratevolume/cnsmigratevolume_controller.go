/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cnsmigratevolume

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	k8sconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
	k8stypes "k8s.io/apimachinery/pkg/types"
	csifault "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/fault"

	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsmigratevolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsmigratevolume/v1alpha1"
	cnsregistervolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsregistervolume/v1alpha1"
	cnsnode "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/node"
	cnsvsphere "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/vsphere"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/common"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/syncer"
)

const (
	defaultMaxWorkerThreadsForMigrateVolume = 10
)

// backOffDuration is a map of cnsnodevmattachment name's to the time after
// which a request for this instance will be requeued.
// Initialized to 1 second for new instances and for instances whose latest
// reconcile operation succeeded.
// If the reconcile fails, backoff is incremented exponentially.
var (
	backOffDuration         map[string]time.Duration
	backOffDurationMapMutex = sync.Mutex{}
)

// Add creates a new CnsMigrateVolume Controller and adds it to the Manager,
// vSphereSecretConfigInfo and VirtualCenterTypes. The Manager will set fields
// on the Controller and Start it when the Manager is Started.
func Add(mgr manager.Manager, clusterFlavor cnstypes.CnsClusterFlavor,
	configInfo *config.ConfigurationInfo, volumeManager volumes.Manager) error {
	ctx, log := logger.GetNewContextWithLogger()
	if clusterFlavor != cnstypes.CnsClusterFlavorWorkload {
		log.Debug("Not initializing the CnsMigrateVolume Controller as its a non-WCP CSI deployment")
		return nil
	}
	log.Info("Initializing the CnsMigrateVolume Controller.")

	// Initializes kubernetes client.
	k8sclient, err := k8s.NewClient(ctx)
	if err != nil {
		log.Errorf("Creating Kubernetes client failed. Err: %v", err)
		return err
	}

	// eventBroadcaster broadcasts events on cnsmigratevolume instances to
	// the event sink.
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(
		&typedcorev1.EventSinkImpl{
			Interface: k8sclient.CoreV1().Events(""),
		},
	)

	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: cnsoperatorapis.GroupName})
	return add(mgr, newReconciler(mgr, configInfo, volumeManager, recorder))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager, configInfo *config.ConfigurationInfo,
	volumeManager volumes.Manager, recorder record.EventRecorder) reconcile.Reconciler {
	ctx, _ := logger.GetNewContextWithLogger()
	return &ReconcileCnsMigrateVolume{client: mgr.GetClient(), scheme: mgr.GetScheme(),
		configInfo: configInfo, volumeManager: volumeManager,
		nodeManager: cnsnode.GetManager(ctx),
		recorder: recorder}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler.
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	ctx, log := logger.GetNewContextWithLogger()
	maxWorkerThreads := getMaxWorkerThreadsToReconcileCnsMigrateVolume(ctx)
	// Create a new controller.
	c, err := controller.New("cnsmigratevolume-controller", mgr,
		controller.Options{Reconciler: r, MaxConcurrentReconciles: maxWorkerThreads})
	if err != nil {
		log.Errorf("failed to create new CnsMigrateVolume controller with error: %+v", err)
		return err
	}

	backOffDuration = make(map[string]time.Duration)

	// Watch for changes to primary resource CnsNodeVmAttachment.
	err = c.Watch(&source.Kind{Type: &cnsmigratevolumev1alpha1.CnsMigrateVolume{}},
		&handler.EnqueueRequestForObject{})
	if err != nil {
		log.Errorf("failed to watch for changes to CnsMigrateVolume resource with error: %+v", err)
		return err
	}
	return nil
}

// blank assignment to verify that ReconcileCnsMigrateVolume implements
// reconcile.Reconciler.
var _ reconcile.Reconciler = &ReconcileCnsMigrateVolume{}

// ReconcileCnsMigrateVolume reconciles a CnsMigrateVolume object.
type ReconcileCnsMigrateVolume struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client           client.Client
	scheme           *runtime.Scheme
	configInfo       *config.ConfigurationInfo
	volumeManager    volumes.Manager
	nodeManager      cnsnode.Manager
	recorder         record.EventRecorder
}

// Reconcile reads that state of the cluster for a CnsMigrateVolume object
// and makes changes based on the state read and what is in the
// CnsMigrateVolume.Spec.
func (r *ReconcileCnsMigrateVolume) Reconcile(ctx context.Context,
	request reconcile.Request) (reconcile.Result, error) {
	ctx = logger.NewContextWithLogger(ctx)
	log := logger.GetLogger(ctx)
	reconcileCnsMigrateVolumeInternal := func() (
		reconcile.Result, error) {
		// Fetch the CnsMigrateVolume instance
		instance := &cnsmigratevolumev1alpha1.CnsMigrateVolume{}
		err := r.client.Get(ctx, request.NamespacedName, instance)
		if err != nil {
			if apierrors.IsNotFound(err) {
				log.Info("CnsMigrateVolume resource not found. Ignoring since object must be deleted.")
				return reconcile.Result{}, nil
			}
			log.Errorf("Error reading the CnsMigrateVolume with name: %q on namespace: %q. Err: %+v",
				request.Name, request.Namespace, err)
			// Error reading the object - return with err.
			return reconcile.Result{}, err
		}

		// Initialize backOffDuration for the instance, if required.
		backOffDurationMapMutex.Lock()
		var timeout time.Duration
		if _, exists := backOffDuration[instance.Name]; !exists {
			backOffDuration[instance.Name] = time.Second
		}
		timeout = backOffDuration[instance.Name]
		backOffDurationMapMutex.Unlock()
		log.Infof("Reconciling CnsMigrateVolume with Request.Name: %q instance %q timeout %q seconds",
			request.Name, instance.Name, timeout)

		// If the CnsMigrateVolume instance is already migrated and
		// not deleted by the user, remove the instance from the queue.
		if instance.Status.Migrated && instance.DeletionTimestamp == nil {
			// This is an upgrade scenario : In summary, we fetch the SV PVC and check if the
			// CNS PVC protection finalizer exist.
			// TODO: add cnsPvcFinalizer logic

			log.Infof("CnsMigrateVolume instance %q status is already migrated. Removing from the queue.", instance.Name)
			// Cleanup instance entry from backOffDuration map.
			backOffDurationMapMutex.Lock()
			delete(backOffDuration, instance.Name)
			backOffDurationMapMutex.Unlock()
			return reconcile.Result{}, nil
		}

		if !instance.Status.Migrated && instance.DeletionTimestamp == nil {
			volumeID, _, err := getVolumeID(ctx, r.client, instance.Spec.VolumeName, instance.Namespace)
			log.Infof("getVolumeID volumeName: %v, volumeID: %v", instance.Spec.VolumeName, volumeID)
			if err != nil {
			    msg := fmt.Sprintf("failed to get volumeID from volumeName: %q for CnsMigrateVolume "+
            	                   "request with name: %q on namespace: %q. Error: %+v",
            	                   instance.Spec.VolumeName, request.Name, request.Namespace, err)
			    instance.Status.Error = err.Error()
			    err = updateCnsMigrateVolume(ctx, r.client, instance)
			    if err != nil {
			        log.Errorf("updateCnsMigrateVolume failed. err: %v", err)
			    }
			    recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
			    return reconcile.Result{RequeueAfter: timeout}, err
			}

			// TODO: add cnsFinalizerExists logic

			// Get a config to talk to the apiserver
			restConfig, err := k8sconfig.GetConfig()
			if err != nil {
				log.Errorf("failed to get Kubernetes config. err: %v", err)
				msg := fmt.Sprintf("failed to get Kubernetes config. err: %v", err)
				err = updateCnsMigrateVolume(ctx, r.client, instance)
				if err != nil {
					log.Errorf("updateCnsMigrateVolume failed. err: %v", err)
				}
				recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
            	return reconcile.Result{RequeueAfter: timeout}, err
			}

			vcenter, err := cnsvsphere.GetVirtualCenterInstance(ctx, r.configInfo, false)
			if err != nil {
				msg := fmt.Sprintf("failed to get virtual center instance with error: %v", err)
				instance.Status.Error = err.Error()
				err = updateCnsMigrateVolume(ctx, r.client, instance)
				if err != nil {
					log.Errorf("updateCnsMigrateVolume failed. err: %v", err)
				}
				recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
				return reconcile.Result{RequeueAfter: timeout}, err
			}
			err = vcenter.Connect(ctx)
			if err != nil {
				msg := fmt.Sprintf("failed to connect to VC with error: %v", err)
				instance.Status.Error = err.Error()
				err = updateCnsMigrateVolume(ctx, r.client, instance)
				if err != nil {
					log.Errorf("updateCnsMigrateVolume failed. err: %v", err)
				}
				recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
				return reconcile.Result{RequeueAfter: timeout}, err
			}

			//verify datastoreUrl is accessible
			err = isDatastoreAccessible(ctx, vcenter, r.configInfo.Cfg.Global.ClusterID,
						instance.Spec.DatastoreUrl)
			if err != nil {
				msg := fmt.Sprintf("failed to connect to VC with error: %v", err)
				instance.Status.Error = err.Error()
				err = updateCnsMigrateVolume(ctx, r.client, instance)
				if err != nil {
					log.Errorf("updateCnsMigrateVolume failed. err: %v", err)
				}
				recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
				return reconcile.Result{RequeueAfter: timeout}, err
			}

			// TODO: check datastore Url with storage class if storageClass is provided;
			//       in the future, we might enhance to recommend one datastore from storageClass
            sourceDatastoreUrl := ""
			if (instance.Spec.Namespace != "") {
				// deregister volume from original namespace
				sourceDatastoreUrl, err = deRegisterVolume(ctx, r.client, r.volumeManager, instance.Spec.VolumeName,
                					volumeID, r.configInfo.Cfg.Global.ClusterID, instance.Namespace)
				if err != nil {
					log.Errorf("PVC %s deregister failed. err: %v",
							instance.Spec.VolumeName, err)
					msg := fmt.Sprintf("PVC %s deregister failed with error: %v",
							instance.Spec.VolumeName, err)
					instance.Status.Error = err.Error()
					err = updateCnsMigrateVolume(ctx, r.client, instance)
					if err != nil {
						log.Errorf("updateCnsMigrateVolume deregister failed. err: %v", err)
					}
					recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
					return reconcile.Result{RequeueAfter: timeout}, err
				}
			}

			if (sourceDatastoreUrl != instance.Spec.DatastoreUrl) {
				log.Infof("volumeID: %v relocate from %s to %s", volumeID, sourceDatastoreUrl,
					instance.Spec.DatastoreUrl)
				err = relocateVolume(ctx, r.volumeManager, vcenter, volumeID, instance.Spec.DatastoreUrl)
				if err != nil {
					msg := fmt.Sprintf("failed to relocate volume with error: %v", err)
					instance.Status.Error = err.Error()
					err = updateCnsMigrateVolume(ctx, r.client, instance)
					if err != nil {
						log.Errorf("updateCnsMigrateVolume relocate failed. err: %v", err)
					}
					recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
					return reconcile.Result{RequeueAfter: timeout}, err
				}
			}

			if (instance.Spec.Namespace != "") {
				// register volume to the new namespace
				err = registerVolume(ctx, restConfig, instance.Spec.Namespace,
                	volumeID, "", instance.Spec.VolumeName, v1.ReadWriteOnce)
				if err != nil {
					log.Errorf("register volume failed. err: %v", err)
					msg := fmt.Sprintf("register volume failed. err: %v", err)
					err = updateCnsMigrateVolume(ctx, r.client, instance)
					if err != nil {
						log.Errorf("updateCnsMigrateVolume register failed. err: %v", err)
					}
					recordEvent(ctx, r, instance, v1.EventTypeWarning, msg)
					return reconcile.Result{RequeueAfter: timeout}, err
				}
			}

			instance.Status.Migrated = true
			instance.Status.Error = ""

			// Cleanup instance entry from backOffDuration map.
			backOffDurationMapMutex.Lock()
			delete(backOffDuration, instance.Name)
			backOffDurationMapMutex.Unlock()
			return reconcile.Result{}, nil
		}

		if instance.DeletionTimestamp != nil {
			// TODO: add logic to handle with this case
		}
		log.Infof("DeletionTimestamp is not empty volumeName: %v", instance.Spec.VolumeName)

		// Cleanup instance entry from backOffDuration map.
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, instance.Name)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}

	resp, err := reconcileCnsMigrateVolumeInternal()
	return resp, err
}

func updateMetadataForDeregister(ctx context.Context,
			volumeManager volumes.Manager, volumeID string,
			clusterID string) (string, error) {
    log := logger.GetLogger(ctx)

	queryFilter := cnstypes.CnsQueryFilter{
					VolumeIds: []cnstypes.CnsVolumeId{{Id: volumeID}},
	}
	// Select volume type, volume name and metadata.
    querySelection := cnstypes.CnsQuerySelection{
    	Names: []string{
    		string(cnstypes.QuerySelectionNameTypeVolumeType),
        	string(cnstypes.QuerySelectionNameTypeDataStoreUrl),
    		"VOLUME_METADATA",
    		},
    	}
	// Query with empty selection. CNS returns only the volume ID from
	// its cache.
	queryAllResult, err := volumeManager.QueryVolumeAsync(ctx, queryFilter, &querySelection)
	if err != nil {
		log.Errorf("PVC deregister: QueryVolume failed for volume %q with err=%+v", volumeID, err.Error())
		// If volume is not found from cns, then it is deregistered.
		return "", nil
	}

	log.Infof("PVC deregister: QueryVolume for volume %s result: %+v",
			volumeID, spew.Sdump(queryAllResult))

	if len(queryAllResult.Volumes) != 1 {
	    return "", logger.LogNewErrorf(log,
    			"PVC deregister: QueryVolume failed for volume %s get result %s", volumeID, len(queryAllResult.Volumes))
    }

	volume := queryAllResult.Volumes[0]
	volume.Metadata.ContainerCluster.ClusterId = common.SupervisorClusterIDForDeRegister
	updateSpec := cnstypes.CnsVolumeMetadataUpdateSpec{
				VolumeId: cnstypes.CnsVolumeId{
        						Id: volumeID,
				},
				Metadata: cnstypes.CnsVolumeMetadata{
								ContainerCluster:      volume.Metadata.ContainerCluster,
				},
	}
	log.Infof("PVC deregister: UpdateVolumeMetadata for volume %s with updateSpec: %+v",
				volumeID, spew.Sdump(updateSpec))
	if err := volumeManager.UpdateVolumeMetadata(ctx, &updateSpec); err != nil {
		log.Warnf("PVC deregister: UpdateVolumeMetadata failed while replacing clusterID "+
					"to be deregistered. Error: %+v", err)
		return "", err
	}

	return volume.DatastoreUrl, nil
}

func deRegisterVolume(ctx context.Context, client client.Client,
					volumeManager volumes.Manager, volumeName string,
					volumeID string, clusterID string, namespace string) (string, error) {
    log := logger.GetLogger(ctx)

	datastoreUrl, err := updateMetadataForDeregister(ctx ,volumeManager, volumeID, clusterID)
	if err != nil {
		log.Warnf("PVC deregister: UpdateVolumeMetadata failed while replacing clusterID "+
					"to be deregistered. Error: %+v", err)
		return "", err
	}
	// Get PVC by pvcName from namespace.
	pvc := &v1.PersistentVolumeClaim{}
	err = client.Get(ctx, k8stypes.NamespacedName{Name: volumeName,
			Namespace: namespace}, pvc)
	if err != nil {
		log.Warnf("PVC deregister: query pvc %s failed, Error: %+v", volumeName, err)
		return "", err
	}
	log.Infof("Now deleting pvc: %s",  pvc)
	err = client.Delete(ctx, pvc)
	if err != nil {
		log.Warnf("PVC deregister: delete pvc %s failed, Error: %+v", volumeName, err)
       	return "", err
	}
	return datastoreUrl, nil
	/*
	pvc, err := k8sclient.CoreV1().PersistentVolumeClaims(instance.Namespace).Delete(ctx,
                		pvcSpec, metav1.CreateOptions{})
    */
}

func registerVolume(
	ctx context.Context, restConfig *rest.Config,
	namespace string, volumeID string, vmdkPath string,
	persistentVolumeClaimName string,
	accessMode v1.PersistentVolumeAccessMode) error {
	var (
		cnsRegisterVolume *cnsregistervolumev1alpha1.CnsRegisterVolume
	)
	cnsRegisterVolume = &cnsregistervolumev1alpha1.CnsRegisterVolume{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: common.RegisterVolumeCRDPrefixForMigrate,
			Namespace:    namespace,
		},
		Spec: cnsregistervolumev1alpha1.CnsRegisterVolumeSpec{
			PvcName: persistentVolumeClaimName,
			AccessMode: v1.PersistentVolumeAccessMode(
				accessMode,
			),
		},
	}

	if vmdkPath != "" {
		cnsRegisterVolume.Spec.DiskURLPath = vmdkPath
	}

	if volumeID != "" {
		cnsRegisterVolume.Spec.VolumeID = volumeID
	}

	cnsOperatorClient, err := k8s.NewClientForGroup(ctx, restConfig, cnsoperatorapis.GroupName)
	err = cnsOperatorClient.Create(ctx, cnsRegisterVolume)

	return err

}

func relocateVolume(ctx context.Context,
	volumeManager volumes.Manager, vcenter *cnsvsphere.VirtualCenter, volumeID string, datastoreUrl string) (error) {
    log := logger.GetLogger(ctx)

    // Get datastore object list.
    dsInfoObjList, err := getDatastoreInfoObjList(ctx, vcenter, datastoreUrl)
    if err != nil {
         log.Infof("failed to retrieve datastore object using datastore "+
         "URL %q. Error: %+v", datastoreUrl, err)
         return err
    }

    relocateSpec := cnstypes.NewCnsBlockVolumeRelocateSpec(volumeID, dsInfoObjList[0].Reference())

    resp, err := volumeManager.RelocateVolumeEx(ctx, relocateSpec)
	log.Infof("Return from CNS Relocate API, taskinfo: %v, Error: %v", resp, err)
	if err != nil {
		// TODO: Handle case when target DS is same as source DS, i.e. volume has
		// already relocated.
		return err
	}

	return nil
}

// Helper function to get DatastoreInfo object for given datastoreURL in the given
// virtual center.
func getDatastoreInfoObjList(ctx context.Context, vc *cnsvsphere.VirtualCenter,
	datastoreURL string) ([]*cnsvsphere.DatastoreInfo, error) {
	log := logger.GetLogger(ctx)
	var datastoreInfos []*cnsvsphere.DatastoreInfo
	datacenters, err := vc.ListDatacenters(ctx)
	if err != nil {
		return nil, err
	}
	var datastoreInfoObj *cnsvsphere.DatastoreInfo
	for _, datacenter := range datacenters {
		datastoreInfoObj, err = datacenter.GetDatastoreInfoByURL(ctx, datastoreURL)
		if err != nil {
			log.Warnf("failed to find datastore with URL %q in datacenter %q from VC %q. Error: %+v",
				datastoreURL, datacenter.InventoryPath, vc.Config.Host, err)
		} else {
			datastoreInfos = append(datastoreInfos, datastoreInfoObj)
		}
	}
	if len(datastoreInfos) > 0 {
		return datastoreInfos, nil
	} else {
		return nil, logger.LogNewErrorf(log,
			"Unable to find datastore for datastore URL %s in VC %+v", datastoreURL, vc)
	}
}

// getVolumeID gets the volume ID from the PV that is bound to PVC by pvcName.
func getVolumeID(ctx context.Context, client client.Client, pvcName string,
	namespace string) (string, string, error) {
	log := logger.GetLogger(ctx)
	// Get PVC by pvcName from namespace.
	pvc := &v1.PersistentVolumeClaim{}
	err := client.Get(ctx, k8stypes.NamespacedName{Name: pvcName, Namespace: namespace}, pvc)
	if err != nil {
		log.Errorf("failed to get PVC with volumename: %q on namespace: %q. Err: %+v",
			pvcName, namespace, err)
		return "", csifault.CSIApiServerOperationFault, err
	}

	// Get PV by name.
	pv := &v1.PersistentVolume{}
	err = client.Get(ctx, k8stypes.NamespacedName{Name: pvc.Spec.VolumeName, Namespace: ""}, pv)
	if err != nil {
		log.Errorf("failed to get PV with name: %q for PVC: %q. Err: %+v",
			pvc.Spec.VolumeName, pvcName, err)
		return "", csifault.CSIPvNotFoundInPvcSpecFault, err
	}
	return pv.Spec.CSI.VolumeHandle, "", nil
}

func isDatastoreAccessible(ctx context.Context, vc *cnsvsphere.VirtualCenter, clusterID string, datastoreUrl string) (error) {
	log := logger.GetLogger(ctx)
	if syncer.IsPodVMOnStretchSupervisorFSSEnabled {
		// TODO: Add logic for IsPodVMOnStretchSupervisorFSSEnabled and len(clusterComputeResourceMoIds) > 1
		/*
		azClustersMap := topologyMgr.GetAZClustersMap(ctx)
		isAccessible := isDatastoreAccessibleToAZClusters(ctx, vc, azClustersMap, datastoreUrl)
		if !isAccessible {
			return logger.LogNewErrorf(log,
						"datastore: %s  is not accessible to any of the AZ clusters: %v",
						datastoreURL, azClustersMap)
		}
		*/
		return nil
	} else {
		// Verify if the volume is accessible to Supervisor cluster.
		isAccessible := isDatastoreAccessibleToCluster(ctx, vc, clusterID, datastoreUrl)
		if !isAccessible {
			log.Errorf("datastore: %s is not accessible to all nodes in the cluster: %s",
				datastoreUrl, clusterID)
			return logger.LogNewErrorf(log,
									"datastore: %s is not accessible to all nodes in the cluster: %v",
									datastoreUrl, clusterID)
		}
		return nil
	}
}

// isDatastoreAccessibleToCluster verifies if the datastoreUrl is accessible to
// cluster with clusterID.
func isDatastoreAccessibleToCluster(ctx context.Context, vc *cnsvsphere.VirtualCenter,
	clusterID string, datastoreURL string) bool {
	log := logger.GetLogger(ctx)
	sharedDatastores, _, err := cnsvsphere.GetCandidateDatastoresInCluster(ctx, vc, clusterID, false)
	if err != nil {
		log.Errorf("Failed to get candidate datastores for cluster: %s with err: %+v", clusterID, err)
		return false
	}
	for _, ds := range sharedDatastores {
		if ds.Info.Url == datastoreURL {
			log.Infof("Found datastoreUrl: %s is accessible to cluster: %s", datastoreURL, clusterID)
			return true
		}
	}
	return false
}

func updateCnsMigrateVolume(ctx context.Context, client client.Client,
	instance *cnsmigratevolumev1alpha1.CnsMigrateVolume) error {
	log := logger.GetLogger(ctx)
	err := client.Update(ctx, instance)
	if err != nil {
		if apierrors.IsConflict(err) {
			log.Infof("Observed conflict while updating CnsMigrateVolume instance %q in namespace %q."+
				"Reapplying changes to the latest instance.", instance.Name, instance.Namespace)

			// Fetch the latest instance version from the API server and apply changes on top of it.
			latestInstance := &cnsmigratevolumev1alpha1.CnsMigrateVolume{}
			err = client.Get(ctx, k8stypes.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}, latestInstance)
			if err != nil {
				log.Errorf("Error reading the CnsMigrateVolume with name: %q on namespace: %q. Err: %+v",
					instance.Name, instance.Namespace, err)
				// Error reading the object - return error
				return err
			}

			// The callers of updateCnsMigrateVolume are either updating the instance finalizers or
			// one of the fields in instance status.
			// Hence we copy only finalizers and Status from the instance passed for update
			// on the latest instance from API server.
			latestInstance.Finalizers = instance.Finalizers
			latestInstance.Status = *instance.Status.DeepCopy()

			err := client.Update(ctx, latestInstance)
			if err != nil {
				log.Errorf("failed to update CnsMigrateVolume instance: %q on namespace: %q. Error: %+v",
					instance.Name, instance.Namespace, err)
				return err
			}
			return nil
		} else {
			log.Errorf("failed to update CnsMigrateVolume instance: %q on namespace: %q. Error: %+v",
				instance.Name, instance.Namespace, err)
		}
	}
	return err
}

// getMaxWorkerThreadsToReconcileCnsMigrateVolume returns the maximum
// number of worker threads which can be run to reconcile CnsMigrateVolume
// instances. If environment variable WORKER_THREADS_MIGRATE_VOLUME is set and
// valid, return the value read from environment variable otherwise, use the
// default value.
func getMaxWorkerThreadsToReconcileCnsMigrateVolume(ctx context.Context) int {
	log := logger.GetLogger(ctx)
	workerThreads := defaultMaxWorkerThreadsForMigrateVolume
	if v := os.Getenv("WORKER_THREADS_MIGRATE_VOLUME"); v != "" {
		if value, err := strconv.Atoi(v); err == nil {
			if value <= 0 {
				log.Warnf("Maximum number of worker threads to run set in env variable "+
					"WORKER_THREADS_MIGRATE_VOLUME %s is less than 1, will use the default value %d",
					v, defaultMaxWorkerThreadsForMigrateVolume)
			} else if value > defaultMaxWorkerThreadsForMigrateVolume {
				log.Warnf("Maximum number of worker threads to run set in env variable "+
					"WORKER_THREADS_MIGRATE_VOLUME %s is greater than %d, will use the default value %d",
					v, defaultMaxWorkerThreadsForMigrateVolume, defaultMaxWorkerThreadsForMigrateVolume)
			} else {
				workerThreads = value
				log.Debugf("Maximum number of worker threads to run to reconcile CnsMigrateVolume "+
					"instances is set to %d", workerThreads)
			}
		} else {
			log.Warnf("Maximum number of worker threads to run set in env variable "+
				"WORKER_THREADS_MIGRATE_VOLUME %s is invalid, will use the default value %d",
				v, defaultMaxWorkerThreadsForMigrateVolume)
		}
	} else {
		log.Debugf("WORKER_THREADS_MIGRATE_VOLUME is not set. Picking the default value %d",
			defaultMaxWorkerThreadsForMigrateVolume)
	}
	return workerThreads
}

// recordEvent records the event, sets the backOffDuration for the instance
// appropriately and logs the message.
// backOffDuration is reset to 1 second on success and doubled on failure.
func recordEvent(ctx context.Context, r *ReconcileCnsMigrateVolume,
	instance *cnsmigratevolumev1alpha1.CnsMigrateVolume, eventtype string, msg string) {
	log := logger.GetLogger(ctx)
	switch eventtype {
	case v1.EventTypeWarning:
		// Double backOff duration.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = backOffDuration[instance.Name] * 2
		backOffDurationMapMutex.Unlock()
		r.recorder.Event(instance, v1.EventTypeWarning, "NodeMigrateVolumeFailed", msg)
		log.Error(msg)
	case v1.EventTypeNormal:
		// Reset backOff duration to one second.
		backOffDurationMapMutex.Lock()
		backOffDuration[instance.Name] = time.Second
		backOffDurationMapMutex.Unlock()
		r.recorder.Event(instance, v1.EventTypeNormal, "NodeMigrateVolumeSucceeded", msg)
		log.Info(msg)
	}
}