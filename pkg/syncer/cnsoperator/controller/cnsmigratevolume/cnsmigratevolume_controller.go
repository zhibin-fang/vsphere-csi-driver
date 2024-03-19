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
	"os"
	"strconv"
	"sync"
	"time"

	cnstypes "github.com/vmware/govmomi/cns/types"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	cnsoperatorapis "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator"
	cnsmigratevolumev1alpha1 "sigs.k8s.io/vsphere-csi-driver/v3/pkg/apis/cnsoperator/cnsmigratevolume/v1alpha1"
	cnsnode "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/node"
	volumes "sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/cns-lib/volume"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/common/config"
	"sigs.k8s.io/vsphere-csi-driver/v3/pkg/csi/service/logger"
	k8s "sigs.k8s.io/vsphere-csi-driver/v3/pkg/kubernetes"
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

        // TODO: add migrate logic here
		instance.Status.Migrated = true
		instance.Status.Error = ""

        // Cleanup instance entry from backOffDuration map.
		backOffDurationMapMutex.Lock()
		delete(backOffDuration, instance.Name)
		backOffDurationMapMutex.Unlock()
		return reconcile.Result{}, nil
	}
	resp, err := reconcileCnsMigrateVolumeInternal()
	return resp, err
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