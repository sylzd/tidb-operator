// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package member

import (
	"fmt"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/Masterminds/semver"
	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/controller"
	"github.com/pingcap/tidb-operator/pkg/label"
	"github.com/pingcap/tidb-operator/pkg/manager"
	"github.com/pingcap/tidb-operator/pkg/util"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/klog"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/utils/pointer"
)

const (
	// pdDataVolumeMountPath is the mount path for pd data volume
	pdDataVolumeMountPath = "/var/lib/pd"

	// pdClusterCertPath is where the cert for inter-cluster communication stored (if any)
	pdClusterCertPath  = "/var/lib/pd-tls"
	tidbClientCertPath = "/var/lib/tidb-client-tls"

	//find a better way to manage store only managed by pd in Operator
	pdMemberLimitPattern = `%s-pd-\d+\.%s-pd-peer\.%s\.svc%s\:\d+`
)

type pdMemberManager struct {
	deps     *controller.Dependencies
	scaler   Scaler
	upgrader Upgrader
	failover Failover
}

// NewPDMemberManager returns a *pdMemberManager
func NewPDMemberManager(dependencies *controller.Dependencies, pdScaler Scaler, pdUpgrader Upgrader, pdFailover Failover) manager.Manager {
	return &pdMemberManager{
		deps:     dependencies,
		scaler:   pdScaler,
		upgrader: pdUpgrader,
		failover: pdFailover,
	}
}

func (m *pdMemberManager) Sync(tc *v1alpha1.TidbCluster) error {
	// If pd is not specified return
	if tc.Spec.PD == nil {
		return nil
	}

	// Sync PD Service
	if err := m.syncPDServiceForTidbCluster(tc); err != nil {
		return err
	}

	// Sync PD Headless Service
	if err := m.syncPDHeadlessServiceForTidbCluster(tc); err != nil {
		return err
	}

	// Sync PD StatefulSet
	return m.syncPDStatefulSetForTidbCluster(tc)
}

func (m *pdMemberManager) syncPDServiceForTidbCluster(tc *v1alpha1.TidbCluster) error {
	if tc.Spec.Paused {
		klog.V(4).Infof("tidb cluster %s/%s is paused, skip syncing for pd service", tc.GetNamespace(), tc.GetName())
		return nil
	}

	ns := tc.GetNamespace()
	tcName := tc.GetName()

	newSvc := m.getNewPDServiceForTidbCluster(tc)
	oldSvcTmp, err := m.deps.ServiceLister.Services(ns).Get(controller.PDMemberName(tcName))
	if errors.IsNotFound(err) {
		err = controller.SetServiceLastAppliedConfigAnnotation(newSvc)
		if err != nil {
			return err
		}
		return m.deps.ServiceControl.CreateService(tc, newSvc)
	}
	if err != nil {
		return fmt.Errorf("syncPDServiceForTidbCluster: failed to get svc %s for cluster %s/%s, error: %s", controller.PDMemberName(tcName), ns, tcName, err)
	}

	oldSvc := oldSvcTmp.DeepCopy()

	equal, err := controller.ServiceEqual(newSvc, oldSvc)
	if err != nil {
		return err
	}
	if !equal {
		svc := *oldSvc
		svc.Spec = newSvc.Spec
		// TODO add unit test
		err = controller.SetServiceLastAppliedConfigAnnotation(&svc)
		if err != nil {
			return err
		}
		svc.Spec.ClusterIP = oldSvc.Spec.ClusterIP
		_, err = m.deps.ServiceControl.UpdateService(tc, &svc)
		return err
	}

	return nil
}

func (m *pdMemberManager) syncPDHeadlessServiceForTidbCluster(tc *v1alpha1.TidbCluster) error {
	if tc.Spec.Paused {
		klog.V(4).Infof("tidb cluster %s/%s is paused, skip syncing for pd headless service", tc.GetNamespace(), tc.GetName())
		return nil
	}

	ns := tc.GetNamespace()
	tcName := tc.GetName()

	newSvc := getNewPDHeadlessServiceForTidbCluster(tc)
	oldSvc, err := m.deps.ServiceLister.Services(ns).Get(controller.PDPeerMemberName(tcName))
	if errors.IsNotFound(err) {
		err = controller.SetServiceLastAppliedConfigAnnotation(newSvc)
		if err != nil {
			return err
		}
		return m.deps.ServiceControl.CreateService(tc, newSvc)
	}
	if err != nil {
		return fmt.Errorf("syncPDHeadlessServiceForTidbCluster: failed to get svc %s for cluster %s/%s, error: %s", controller.PDPeerMemberName(tcName), ns, tcName, err)
	}

	equal, err := controller.ServiceEqual(newSvc, oldSvc)
	if err != nil {
		return err
	}
	if !equal {
		svc := *oldSvc
		svc.Spec = newSvc.Spec
		err = controller.SetServiceLastAppliedConfigAnnotation(&svc)
		if err != nil {
			return err
		}
		_, err = m.deps.ServiceControl.UpdateService(tc, &svc)
		return err
	}

	return nil
}

// lzd: 完成 Service 的同步后，组件接入了集群的网络，可以在集群内访问和被访问。控制循环会进入 syncStatefulSetForTidbCluster，开始对 Statefulset 进行 Reconcile，首先是使用 syncTidbClusterStatus 对组件的 Status 信息进行同步，后续的扩缩容、Failover、升级等操作会依赖 Status 中的信息进行决策。
func (m *pdMemberManager) syncPDStatefulSetForTidbCluster(tc *v1alpha1.TidbCluster) error {
	ns := tc.GetNamespace()
	tcName := tc.GetName()

	oldPDSetTmp, err := m.deps.StatefulSetLister.StatefulSets(ns).Get(controller.PDMemberName(tcName))
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("syncPDStatefulSetForTidbCluster: fail to get sts %s for cluster %s/%s, error: %s", controller.PDMemberName(tcName), ns, tcName, err)
	}
	setNotExist := errors.IsNotFound(err)

	oldPDSet := oldPDSetTmp.DeepCopy()

	// lzd 1. 同步组件status：同步 Status 是 TiDB Operator 比较关键的操作，它需要同步 Kubernetes 各个组件的信息和 TiDB 的集群内部信息，例如在 Kubernetes 方面，在这一操作中会同步集群的副本数量，更新状态，镜像版本等信息，检查 Statefulset 是否处于更新状态。在 TiDB 集群信息方面，TiDB Operator 还需要将 TiDB 集群内部的信息从 PD 中同步下来。例如 PD 的成员信息，TiKV 的存储信息，TiDB 的成员信息等，TiDB 集群的健康检查的操作便是在更新 Status 这一操作内完成。
	if err := m.syncTidbClusterStatus(tc, oldPDSet); err != nil {
		klog.Errorf("failed to sync TidbCluster: [%s/%s]'s status, error: %v", ns, tcName, err)
	}

	// lzd 2. 检查TiDBCluster是否暂停同步：更新完状态后，会通过 tc.Spec.Paused 判断集群是否处于暂停同步状态，如果暂停同步，则会跳过下面更新 Statefulset 的操作。
	if tc.Spec.Paused {
		klog.V(4).Infof("tidb cluster %s/%s is paused, skip syncing for pd statefulset", tc.GetNamespace(), tc.GetName())
		return nil
	}

	// lzd 3. 同步configmap：在同步完 Status 之后，syncConfigMap 函数会更新 ConfigMap，ConfigMap 一般包括了组件的配置文件和启动脚本。配置文件是通过 YAML 中 Spec 的 Config 项提取而来，目前支持 TOML 透传和 YAML 转换，并且推荐 TOML 格式。启动脚本则包含了获取组件所需的启动参数，然后用获取好的启动参数启动组件进程。在 TiDB Operator 中，当组件启动时需要向 TiDB Operator 获取启动参数时，TiDB Operator 侧的信息处理会放到 discovery 组件完成。如 PD 获取参数用于决定初始化还是加入某个节点，就会使用 wget 访问 discovery 拿到自己需要的参数。这种在启动脚本中获取参数的方法，避免了更新 Statefulset 过程中引起的非预期滚动更新，对线上业务造成影响。
	cm, err := m.syncPDConfigMap(tc, oldPDSet)
	if err != nil {
		return err
	}

	// lzd 4. 根据 TidbCluster 配置生成新的 Statefulset，并且对新 Statefulset 进行滚动更新，扩缩容，Failover 相关逻辑的处理：
	// lzd 4.1 getNewPDSetForTidbCluster 函数会得到一个新的 Statefulset 的模板，它包含了对刚才生成的 Service，ConfigMap 的引用，并且根据最新的 Status 和 Spec 生成其他项，例如 env，container，volume 等
	newPDSet, err := getNewPDSetForTidbCluster(tc, cm)
	if err != nil {
		return err
	}
	if setNotExist {
		err = SetStatefulSetLastAppliedConfigAnnotation(newPDSet)
		if err != nil {
			return err
		}
		if err := m.deps.StatefulSetControl.CreateStatefulSet(tc, newPDSet); err != nil {
			return err
		}
		tc.Status.PD.StatefulSet = &apps.StatefulSetStatus{}
		return controller.RequeueErrorf("TidbCluster: [%s/%s], waiting for PD cluster running", ns, tcName)
	}

	// lzd 4.2 Upgrade 函数负责滚动更新相关操作，主要更新 Statefulset 中 UpgradeStrategy.Type 和 UpgradeStrategy.Partition，滚动更新是借助 Statefulset 的 RollingUpdate 策略实现的。组件 Reconcile 会设置 Statefulset 的升级策略为滚动更新，即组件 Statefulset 的 UpgradeStrategy.Type 设置为 RollingUpdate 。在 Kubernetes 的 Statefulset 使用中，可以通过配置 UpgradeStrategy.Partition 控制滚动更新的进度，即 Statefulset 只会更新序号大于或等于 partition 的值，并且未被更新的 Pod。通过这一机制就可以实现每个 Pod 在正常对外服务后才继续滚动更新。在非升级状态或者升级的启动阶段，组件的 Reconcile 会将 Statefulset 的 UpgradeStrategy.Partition 设置为 Statefulset 中最大的 Pod 序号，防止有 Pod 更新。在开始更新后，当一个 Pod 更新，并且重启后对外提供服务，例如 TiKV 的 Store 状态变为 Up，TiDB 的 Member 状态为 healthy，满足这样的条件的 Pod 才被视为升级成功，然后调小 Partition 的值进行下一 Pod 的更新
	// Force update takes precedence over scaling because force upgrade won't take effect when cluster gets stuck at scaling
	if !tc.Status.PD.Synced && NeedForceUpgrade(tc.Annotations) {
		tc.Status.PD.Phase = v1alpha1.UpgradePhase
		setUpgradePartition(newPDSet, 0)
		errSTS := UpdateStatefulSet(m.deps.StatefulSetControl, tc, newPDSet, oldPDSet)
		return controller.RequeueErrorf("tidbcluster: [%s/%s]'s pd needs force upgrade, %v", ns, tcName, errSTS)
	}

	// lzd 4.3 m.scaler.Scale 函数负责扩缩容相关操作，主要是更新 Statefulset 中组件的 Replicas。Scale 遵循逐个扩缩容的原则，每次扩缩容的跨度为 1。Scale 函数会将 TiDBCluster 中指定的组件 Replicas 数，如 tc.Spec.PD.Replicas，与现有比较，先判断是扩容需求还是缩容需求，然后完成一个步长的扩缩容的操作，再进入下一次组件 Reconcile，通过多次 Reconcile 完成所有的扩缩容需求。在扩缩容的过程中，会涉及到 PD 转移 Leader，TiKV 删除 Store 等使用 PD API 的操作，组件 Reconcile 过程中会使用 PD API 完成上述操作，并且判断操作是否成功，再逐步进行下一次扩缩容。
	// Scaling takes precedence over upgrading because:
	// - if a pd fails in the upgrading, users may want to delete it or add
	//   new replicas
	// - it's ok to scale in the middle of upgrading (in statefulset controller
	//   scaling takes precedence over upgrading too)
	if err := m.scaler.Scale(tc, oldPDSet, newPDSet); err != nil {
		return err
	}

	// lzd 4.4 m.failover.Failover 函数负责容灾相关的操作，包括发现和记录灾难状态，恢复灾难状态等，在部署 TiDB Operator 时配置打开 AutoFailover 后，当发现有 Failure 的组件时记录相关信息到 FailureStores 或者 FailureMembers 这样的状态存储的键值，并启动一个新的组件 Pod 用于承担这个 Pod 的工作负载。当原 Pod 恢复工作后，通过修改 Statefulset 的 Replicas 数量，将用于容灾时分担工作负载的 Pod 进行缩容操作。但是在 TiKV/TiFlash 的容灾逻辑中，自动缩容容灾过程中的 Pod 不是默认操作，需要设置 spec.tikv.recoverFailover: true 才会对新启动的 Pod 缩容
	if m.deps.CLIConfig.AutoFailover {
		if m.shouldRecover(tc) {
			m.failover.Recover(tc)
		} else if tc.PDAllPodsStarted() && !tc.PDAllMembersReady() || tc.PDAutoFailovering() {
			if err := m.failover.Failover(tc); err != nil {
				return err
			}
		}
	}

	if !templateEqual(newPDSet, oldPDSet) || tc.Status.PD.Phase == v1alpha1.UpgradePhase {
		if err := m.upgrader.Upgrade(tc, oldPDSet, newPDSet); err != nil {
			return err
		}
	}

	// lzd 5. 创建或者更新 Statefulset：在同步 Statefulset 的最后阶段，已经完成了新 Statefulset 的生成，这时候会进入 UpdateStatefulSet 函数，这一函数中主要比对新的 Statefulset 和现有 StatefulSet 差异，如果不一致，则进行 Statefulset 的实际更新。另外，这一函数还需要检查是否有没有被管理的 Statefulset，这部分主要是旧版本使用 Helm Chart 部署的 TiDB，需要将这些 Statefulset 纳入 TiDB Operator 的管理，给他们添加依赖标记。
	return UpdateStatefulSet(m.deps.StatefulSetControl, tc, newPDSet, oldPDSet)

	// lzd 6. 完成上述操作后，TiDBCluster CR 的 Status 更新到最新，相关 Service，ConfigMap 被创建，生成了新的 Statefulset，并且满足了滚动更新，扩缩容，Failover 的工作。组件的 Reconcile 周而复始，监控着组件的生命周期状态，响应生命周期状态改变和用户输入改变，使得集群在符合预期的状态下正常运行。
}

// shouldRecover checks whether we should perform recovery operation.
func (m *pdMemberManager) shouldRecover(tc *v1alpha1.TidbCluster) bool {
	if tc.Status.PD.FailureMembers == nil {
		return false
	}
	// If all desired replicas (excluding failover pods) of tidb cluster are
	// healthy, we can perform our failover recovery operation.
	// Note that failover pods may fail (e.g. lack of resources) and we don't care
	// about them because we're going to delete them.
	for ordinal := range tc.PDStsDesiredOrdinals(true) {
		name := fmt.Sprintf("%s-%d", controller.PDMemberName(tc.GetName()), ordinal)
		pod, err := m.deps.PodLister.Pods(tc.Namespace).Get(name)
		if err != nil {
			klog.Errorf("pod %s/%s does not exist: %v", tc.Namespace, name, err)
			return false
		}
		if !podutil.IsPodReady(pod) {
			return false
		}
		ok := false
		for pdName, pdMember := range tc.Status.PD.Members {
			if strings.Split(pdName, ".")[0] == pod.Name {
				if !pdMember.Health {
					return false
				}
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func (m *pdMemberManager) syncTidbClusterStatus(tc *v1alpha1.TidbCluster, set *apps.StatefulSet) error {
	if set == nil {
		// skip if not created yet
		return nil
	}

	ns := tc.GetNamespace()
	tcName := tc.GetName()

	tc.Status.PD.StatefulSet = &set.Status

	upgrading, err := m.pdStatefulSetIsUpgrading(set, tc)
	if err != nil {
		return err
	}

	// Scaling takes precedence over upgrading.
	if tc.PDStsDesiredReplicas() != *set.Spec.Replicas {
		tc.Status.PD.Phase = v1alpha1.ScalePhase
	} else if upgrading {
		tc.Status.PD.Phase = v1alpha1.UpgradePhase
	} else {
		tc.Status.PD.Phase = v1alpha1.NormalPhase
	}

	pdClient := controller.GetPDClient(m.deps.PDControl, tc)

	healthInfo, err := pdClient.GetHealth()
	if err != nil {
		tc.Status.PD.Synced = false
		// get endpoints info
		eps, epErr := m.deps.EndpointLister.Endpoints(ns).Get(controller.PDMemberName(tcName))
		if epErr != nil {
			return fmt.Errorf("syncTidbClusterStatus: failed to get endpoints %s for cluster %s/%s, err: %s, epErr %s", controller.PDMemberName(tcName), ns, tcName, err, epErr)
		}
		// pd service has no endpoints
		if eps != nil && len(eps.Subsets) == 0 {
			return fmt.Errorf("%s, service %s/%s has no endpoints", err, ns, controller.PDMemberName(tcName))
		}
		return err
	}

	cluster, err := pdClient.GetCluster()
	if err != nil {
		tc.Status.PD.Synced = false
		return err
	}
	tc.Status.ClusterID = strconv.FormatUint(cluster.Id, 10)
	leader, err := pdClient.GetPDLeader()
	if err != nil {
		tc.Status.PD.Synced = false
		return err
	}

	rePDMembers, err := regexp.Compile(fmt.Sprintf(pdMemberLimitPattern, tc.Name, tc.Name, tc.Namespace, controller.FormatClusterDomainForRegex(tc.Spec.ClusterDomain)))
	if err != nil {
		return err
	}
	pdStatus := map[string]v1alpha1.PDMember{}
	peerPDStatus := map[string]v1alpha1.PDMember{}
	for _, memberHealth := range healthInfo.Healths {
		memberID := memberHealth.MemberID
		var clientURL string
		if len(memberHealth.ClientUrls) > 0 {
			clientURL = memberHealth.ClientUrls[0]
		}
		name := memberHealth.Name
		if len(name) == 0 {
			klog.Warningf("PD member: [%d] doesn't have a name, and can't get it from clientUrls: [%s], memberHealth Info: [%v] in [%s/%s]",
				memberID, memberHealth.ClientUrls, memberHealth, ns, tcName)
			continue
		}

		status := v1alpha1.PDMember{
			Name:      name,
			ID:        fmt.Sprintf("%d", memberID),
			ClientURL: clientURL,
			Health:    memberHealth.Health,
		}
		status.LastTransitionTime = metav1.Now()

		// matching `rePDMembers` means `clientURL` is a PD in current tc
		if rePDMembers.Match([]byte(clientURL)) {
			oldPDMember, exist := tc.Status.PD.Members[name]
			if exist && status.Health == oldPDMember.Health {
				status.LastTransitionTime = oldPDMember.LastTransitionTime
			}
			pdStatus[name] = status
		} else {
			oldPDMember, exist := tc.Status.PD.PeerMembers[name]
			if exist && status.Health == oldPDMember.Health {
				status.LastTransitionTime = oldPDMember.LastTransitionTime
			}
			peerPDStatus[name] = status
		}

		if name == leader.GetName() {
			tc.Status.PD.Leader = status
		}
	}

	tc.Status.PD.Synced = true
	tc.Status.PD.Members = pdStatus
	tc.Status.PD.PeerMembers = peerPDStatus
	tc.Status.PD.Image = ""
	if c := findContainerByName(set, "pd"); c != nil {
		tc.Status.PD.Image = c.Image
	}

	if err := m.collectUnjoinedMembers(tc, set, pdStatus); err != nil {
		return err
	}
	return nil
}

// syncPDConfigMap syncs the configmap of PD
func (m *pdMemberManager) syncPDConfigMap(tc *v1alpha1.TidbCluster, set *apps.StatefulSet) (*corev1.ConfigMap, error) {

	// For backward compatibility, only sync tidb configmap when .pd.config is non-nil
	if tc.Spec.PD.Config == nil {
		return nil, nil
	}
	newCm, err := getPDConfigMap(tc)
	if err != nil {
		return nil, err
	}

	var inUseName string
	if set != nil {
		inUseName = FindConfigMapVolume(&set.Spec.Template.Spec, func(name string) bool {
			return strings.HasPrefix(name, controller.PDMemberName(tc.Name))
		})
	}

	err = updateConfigMapIfNeed(m.deps.ConfigMapLister, tc.BasePDSpec().ConfigUpdateStrategy(), inUseName, newCm)
	if err != nil {
		return nil, err
	}
	return m.deps.TypedControl.CreateOrUpdateConfigMap(tc, newCm)
}

func (m *pdMemberManager) getNewPDServiceForTidbCluster(tc *v1alpha1.TidbCluster) *corev1.Service {
	ns := tc.Namespace
	tcName := tc.Name
	svcName := controller.PDMemberName(tcName)
	instanceName := tc.GetInstanceName()
	pdSelector := label.New().Instance(instanceName).PD()
	pdLabels := pdSelector.Copy().UsedByEndUser().Labels()

	pdService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            svcName,
			Namespace:       ns,
			Labels:          pdLabels,
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(tc)},
		},
		Spec: corev1.ServiceSpec{
			Type: controller.GetServiceType(tc.Spec.Services, v1alpha1.PDMemberType.String()),
			Ports: []corev1.ServicePort{
				{
					Name:       "client",
					Port:       2379,
					TargetPort: intstr.FromInt(2379),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Selector: pdSelector.Labels(),
		},
	}
	// if set pd service type ,overwrite global variable services
	svcSpec := tc.Spec.PD.Service
	if svcSpec != nil {
		if svcSpec.Type != "" {
			pdService.Spec.Type = svcSpec.Type
		}
		pdService.ObjectMeta.Annotations = CopyAnnotations(svcSpec.Annotations)
		if svcSpec.LoadBalancerIP != nil {
			pdService.Spec.LoadBalancerIP = *svcSpec.LoadBalancerIP
		}
		if svcSpec.ClusterIP != nil {
			pdService.Spec.ClusterIP = *svcSpec.ClusterIP
		}
		if svcSpec.PortName != nil {
			pdService.Spec.Ports[0].Name = *svcSpec.PortName
		}
	}
	return pdService
}

func getNewPDHeadlessServiceForTidbCluster(tc *v1alpha1.TidbCluster) *corev1.Service {
	ns := tc.Namespace
	tcName := tc.Name
	svcName := controller.PDPeerMemberName(tcName)
	instanceName := tc.GetInstanceName()
	pdSelector := label.New().Instance(instanceName).PD()
	pdLabels := pdSelector.Copy().UsedByPeer().Labels()

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            svcName,
			Namespace:       ns,
			Labels:          pdLabels,
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(tc)},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Ports: []corev1.ServicePort{
				{
					Name:       "peer",
					Port:       2380,
					TargetPort: intstr.FromInt(2380),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Selector:                 pdSelector.Labels(),
			PublishNotReadyAddresses: true,
		},
	}
}

func (m *pdMemberManager) pdStatefulSetIsUpgrading(set *apps.StatefulSet, tc *v1alpha1.TidbCluster) (bool, error) {
	if statefulSetIsUpgrading(set) {
		return true, nil
	}
	instanceName := tc.GetInstanceName()
	selector, err := label.New().
		Instance(instanceName).
		PD().
		Selector()
	if err != nil {
		return false, err
	}
	pdPods, err := m.deps.PodLister.Pods(tc.GetNamespace()).List(selector)
	if err != nil {
		return false, fmt.Errorf("pdStatefulSetIsUpgrading: failed to list pods for cluster %s/%s, selector %s, error: %v", tc.GetNamespace(), instanceName, selector, err)
	}
	for _, pod := range pdPods {
		revisionHash, exist := pod.Labels[apps.ControllerRevisionHashLabelKey]
		if !exist {
			return false, nil
		}
		if revisionHash != tc.Status.PD.StatefulSet.UpdateRevision {
			return true, nil
		}
	}
	return false, nil
}

func getNewPDSetForTidbCluster(tc *v1alpha1.TidbCluster, cm *corev1.ConfigMap) (*apps.StatefulSet, error) {
	ns := tc.Namespace
	tcName := tc.Name
	basePDSpec := tc.BasePDSpec()
	instanceName := tc.GetInstanceName()
	pdConfigMap := controller.MemberConfigMapName(tc, v1alpha1.PDMemberType)
	if cm != nil {
		pdConfigMap = cm.Name
	}

	clusterVersionGE4, err := clusterVersionGreaterThanOrEqualTo4(tc.PDVersion())
	if err != nil {
		klog.V(4).Infof("cluster version: %s is not semantic versioning compatible", tc.PDVersion())
	}

	annMount, annVolume := annotationsMountVolume()
	volMounts := []corev1.VolumeMount{
		annMount,
		{Name: "config", ReadOnly: true, MountPath: "/etc/pd"},
		{Name: "startup-script", ReadOnly: true, MountPath: "/usr/local/bin"},
		{Name: v1alpha1.PDMemberType.String(), MountPath: pdDataVolumeMountPath},
	}
	if tc.IsTLSClusterEnabled() {
		volMounts = append(volMounts, corev1.VolumeMount{
			Name: "pd-tls", ReadOnly: true, MountPath: "/var/lib/pd-tls",
		})
		if tc.Spec.PD.MountClusterClientSecret != nil && *tc.Spec.PD.MountClusterClientSecret {
			volMounts = append(volMounts, corev1.VolumeMount{
				Name: util.ClusterClientVolName, ReadOnly: true, MountPath: util.ClusterClientTLSPath,
			})
		}
	}
	if tc.Spec.TiDB != nil && tc.Spec.TiDB.IsTLSClientEnabled() && !tc.SkipTLSWhenConnectTiDB() && clusterVersionGE4 {
		volMounts = append(volMounts, corev1.VolumeMount{
			Name: "tidb-client-tls", ReadOnly: true, MountPath: tidbClientCertPath,
		})
	}

	vols := []corev1.Volume{
		annVolume,
		{Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: pdConfigMap,
					},
					Items: []corev1.KeyToPath{{Key: "config-file", Path: "pd.toml"}},
				},
			},
		},
		{Name: "startup-script",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: pdConfigMap,
					},
					Items: []corev1.KeyToPath{{Key: "startup-script", Path: "pd_start_script.sh"}},
				},
			},
		},
	}
	if tc.IsTLSClusterEnabled() {
		vols = append(vols, corev1.Volume{
			Name: "pd-tls", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: util.ClusterTLSSecretName(tc.Name, label.PDLabelVal),
				},
			},
		})
		if tc.Spec.PD.MountClusterClientSecret != nil && *tc.Spec.PD.MountClusterClientSecret {
			vols = append(vols, corev1.Volume{
				Name: util.ClusterClientVolName, VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: util.ClusterClientTLSSecretName(tc.Name),
					},
				},
			})
		}
	}
	if tc.Spec.TiDB != nil && tc.Spec.TiDB.IsTLSClientEnabled() && !tc.SkipTLSWhenConnectTiDB() && clusterVersionGE4 {
		clientSecretName := util.TiDBClientTLSSecretName(tc.Name)
		if tc.Spec.PD.TLSClientSecretName != nil {
			clientSecretName = *tc.Spec.PD.TLSClientSecretName
		}
		vols = append(vols, corev1.Volume{
			Name: "tidb-client-tls", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: clientSecretName,
				},
			},
		})
	}
	// handle StorageVolumes and AdditionalVolumeMounts in ComponentSpec
	storageVolMounts, additionalPVCs := util.BuildStorageVolumeAndVolumeMount(tc.Spec.PD.StorageVolumes, tc.Spec.PD.StorageClassName, v1alpha1.PDMemberType)
	volMounts = append(volMounts, storageVolMounts...)
	volMounts = append(volMounts, tc.Spec.PD.AdditionalVolumeMounts...)

	sysctls := "sysctl -w"
	var initContainers []corev1.Container
	if basePDSpec.Annotations() != nil {
		init, ok := basePDSpec.Annotations()[label.AnnSysctlInit]
		if ok && (init == label.AnnSysctlInitVal) {
			if basePDSpec.PodSecurityContext() != nil && len(basePDSpec.PodSecurityContext().Sysctls) > 0 {
				for _, sysctl := range basePDSpec.PodSecurityContext().Sysctls {
					sysctls = sysctls + fmt.Sprintf(" %s=%s", sysctl.Name, sysctl.Value)
				}
				privileged := true
				initContainers = append(initContainers, corev1.Container{
					Name:  "init",
					Image: tc.HelperImage(),
					Command: []string{
						"sh",
						"-c",
						sysctls,
					},
					SecurityContext: &corev1.SecurityContext{
						Privileged: &privileged,
					},
					// Init container resourceRequirements should be equal to app container.
					// Scheduling is done based on effective requests/limits,
					// which means init containers can reserve resources for
					// initialization that are not used during the life of the Pod.
					// ref:https://kubernetes.io/docs/concepts/workloads/pods/init-containers/#resources
					Resources: controller.ContainerResource(tc.Spec.PD.ResourceRequirements),
				})
			}
		}
	}
	// Init container is only used for the case where allowed-unsafe-sysctls
	// cannot be enabled for kubelet, so clean the sysctl in statefulset
	// SecurityContext if init container is enabled
	podSecurityContext := basePDSpec.PodSecurityContext().DeepCopy()
	if len(initContainers) > 0 {
		podSecurityContext.Sysctls = []corev1.Sysctl{}
	}

	storageRequest, err := controller.ParseStorageRequest(tc.Spec.PD.Requests)
	if err != nil {
		return nil, fmt.Errorf("cannot parse storage request for PD, tidbcluster %s/%s, error: %v", tc.Namespace, tc.Name, err)
	}

	pdLabel := label.New().Instance(instanceName).PD()
	setName := controller.PDMemberName(tcName)
	podAnnotations := CombineAnnotations(controller.AnnProm(2379), basePDSpec.Annotations())
	stsAnnotations := getStsAnnotations(tc.Annotations, label.PDLabelVal)

	deleteSlotsNumber, err := util.GetDeleteSlotsNumber(stsAnnotations)
	if err != nil {
		return nil, fmt.Errorf("get delete slots number of statefulset %s/%s failed, err:%v", ns, setName, err)
	}

	pdContainer := corev1.Container{
		Name:            v1alpha1.PDMemberType.String(),
		Image:           tc.PDImage(),
		ImagePullPolicy: basePDSpec.ImagePullPolicy(),
		Command:         []string{"/bin/sh", "/usr/local/bin/pd_start_script.sh"},
		Ports: []corev1.ContainerPort{
			{
				Name:          "server",
				ContainerPort: int32(2380),
				Protocol:      corev1.ProtocolTCP,
			},
			{
				Name:          "client",
				ContainerPort: int32(2379),
				Protocol:      corev1.ProtocolTCP,
			},
		},
		VolumeMounts: volMounts,
		Resources:    controller.ContainerResource(tc.Spec.PD.ResourceRequirements),
	}
	env := []corev1.EnvVar{
		{
			Name: "NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		},
		{
			Name:  "PEER_SERVICE_NAME",
			Value: controller.PDPeerMemberName(tcName),
		},
		{
			Name:  "SERVICE_NAME",
			Value: controller.PDMemberName(tcName),
		},
		{
			Name:  "SET_NAME",
			Value: setName,
		},
		{
			Name:  "TZ",
			Value: tc.Spec.Timezone,
		},
	}

	podSpec := basePDSpec.BuildPodSpec()
	if basePDSpec.HostNetwork() {
		podSpec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
		env = append(env, corev1.EnvVar{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		})
	}
	pdContainer.Env = util.AppendEnv(env, basePDSpec.Env())
	podSpec.Volumes = append(vols, basePDSpec.AdditionalVolumes()...)
	podSpec.Containers = append([]corev1.Container{pdContainer}, basePDSpec.AdditionalContainers()...)
	podSpec.ServiceAccountName = tc.Spec.PD.ServiceAccount
	if podSpec.ServiceAccountName == "" {
		podSpec.ServiceAccountName = tc.Spec.ServiceAccount
	}
	podSpec.SecurityContext = podSecurityContext
	podSpec.InitContainers = append(initContainers, basePDSpec.InitContainers()...)

	updateStrategy := apps.StatefulSetUpdateStrategy{}
	if basePDSpec.StatefulSetUpdateStrategy() == apps.OnDeleteStatefulSetStrategyType {
		updateStrategy.Type = apps.OnDeleteStatefulSetStrategyType
	} else {
		updateStrategy.Type = apps.RollingUpdateStatefulSetStrategyType
		updateStrategy.RollingUpdate = &apps.RollingUpdateStatefulSetStrategy{
			Partition: pointer.Int32Ptr(tc.PDStsDesiredReplicas() + deleteSlotsNumber),
		}
	}

	pdSet := &apps.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            setName,
			Namespace:       ns,
			Labels:          pdLabel.Labels(),
			Annotations:     stsAnnotations,
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(tc)},
		},
		Spec: apps.StatefulSetSpec{
			Replicas: pointer.Int32Ptr(tc.PDStsDesiredReplicas()),
			Selector: pdLabel.LabelSelector(),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      pdLabel.Labels(),
					Annotations: podAnnotations,
				},
				Spec: podSpec,
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: v1alpha1.PDMemberType.String(),
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						StorageClassName: tc.Spec.PD.StorageClassName,
						Resources:        storageRequest,
					},
				},
			},
			ServiceName:         controller.PDPeerMemberName(tcName),
			PodManagementPolicy: apps.ParallelPodManagement,
			UpdateStrategy:      updateStrategy,
		},
	}

	pdSet.Spec.VolumeClaimTemplates = append(pdSet.Spec.VolumeClaimTemplates, additionalPVCs...)
	return pdSet, nil
}

func getPDConfigMap(tc *v1alpha1.TidbCluster) (*corev1.ConfigMap, error) {
	// For backward compatibility, only sync tidb configmap when .tidb.config is non-nil
	config := tc.Spec.PD.Config
	if config == nil {
		return nil, nil
	}

	clusterVersionGE4, err := clusterVersionGreaterThanOrEqualTo4(tc.PDVersion())
	if err != nil {
		klog.V(4).Infof("cluster version: %s is not semantic versioning compatible", tc.PDVersion())
	}

	// override CA if tls enabled
	if tc.IsTLSClusterEnabled() {
		config.Set("security.cacert-path", path.Join(pdClusterCertPath, tlsSecretRootCAKey))
		config.Set("security.cert-path", path.Join(pdClusterCertPath, corev1.TLSCertKey))
		config.Set("security.key-path", path.Join(pdClusterCertPath, corev1.TLSPrivateKeyKey))
	}
	// Versions below v4.0 do not support Dashboard
	if tc.Spec.TiDB != nil && tc.Spec.TiDB.IsTLSClientEnabled() && !tc.SkipTLSWhenConnectTiDB() && clusterVersionGE4 {
		config.Set("dashboard.tidb-cacert-path", path.Join(tidbClientCertPath, tlsSecretRootCAKey))
		config.Set("dashboard.tidb-cert-path", path.Join(tidbClientCertPath, corev1.TLSCertKey))
		config.Set("dashboard.tidb-key-path", path.Join(tidbClientCertPath, corev1.TLSPrivateKeyKey))
	}

	if tc.Spec.PD.EnableDashboardInternalProxy != nil {
		config.Set("dashboard.internal-proxy", *tc.Spec.PD.EnableDashboardInternalProxy)
	}

	confText, err := config.MarshalTOML()
	if err != nil {
		return nil, err
	}
	startScript, err := RenderPDStartScript(&PDStartScriptModel{
		Scheme:        tc.Scheme(),
		DataDir:       filepath.Join(pdDataVolumeMountPath, tc.Spec.PD.DataSubDir),
		ClusterDomain: tc.Spec.ClusterDomain,
	})
	if err != nil {
		return nil, err
	}

	instanceName := tc.GetInstanceName()
	pdLabel := label.New().Instance(instanceName).PD().Labels()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            controller.PDMemberName(tc.Name),
			Namespace:       tc.Namespace,
			Labels:          pdLabel,
			OwnerReferences: []metav1.OwnerReference{controller.GetOwnerRef(tc)},
		},
		Data: map[string]string{
			"config-file":    string(confText),
			"startup-script": startScript,
		},
	}
	return cm, nil
}

func clusterVersionGreaterThanOrEqualTo4(version string) (bool, error) {
	v, err := semver.NewVersion(version)
	if err != nil {
		return true, err
	}

	return v.Major() >= 4, nil
}

// find PD pods in set that have not joined the PD cluster yet.
// pdStatus contains the PD members in the PD cluster.
func (m *pdMemberManager) collectUnjoinedMembers(tc *v1alpha1.TidbCluster, set *apps.StatefulSet, pdStatus map[string]v1alpha1.PDMember) error {
	ns := tc.GetNamespace()
	podSelector, podSelectErr := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if podSelectErr != nil {
		return podSelectErr
	}
	pods, podErr := m.deps.PodLister.Pods(tc.Namespace).List(podSelector)
	if podErr != nil {
		return fmt.Errorf("collectUnjoinedMembers: failed to list pods for cluster %s/%s, selector %s, error %v", tc.GetNamespace(), tc.GetName(), set.Spec.Selector, podErr)
	}

	// check all pods in PD sts to see whether it has already joined the PD cluster
	for _, pod := range pods {
		var joined = false
		// if current PD pod name is in the keys of pdStatus, it has joined the PD cluster
		for pdName := range pdStatus {
			ordinal, err := util.GetOrdinalFromPodName(pod.Name)
			if err != nil {
				return fmt.Errorf("unexpected pod name %q: %v", pod.Name, err)
			}
			if strings.EqualFold(PdName(tc.Name, ordinal, tc.Namespace, tc.Spec.ClusterDomain), pdName) {
				joined = true
				break
			}
		}
		if !joined {
			if tc.Status.PD.UnjoinedMembers == nil {
				tc.Status.PD.UnjoinedMembers = map[string]v1alpha1.UnjoinedMember{}
			}
			pvcs, err := util.ResolvePVCFromPod(pod, m.deps.PVCLister)
			if err != nil {
				return fmt.Errorf("collectUnjoinedMembers: failed to get pvcs for pod %s/%s, error: %s", ns, pod.Name, err)
			}
			pvcUIDSet := make(map[types.UID]struct{})
			for _, pvc := range pvcs {
				pvcUIDSet[pvc.UID] = struct{}{}
			}
			tc.Status.PD.UnjoinedMembers[pod.Name] = v1alpha1.UnjoinedMember{
				PodName:   pod.Name,
				PVCUIDSet: pvcUIDSet,
				CreatedAt: metav1.Now(),
			}
		} else {
			delete(tc.Status.PD.UnjoinedMembers, pod.Name)
		}
	}
	return nil
}

// TODO: seems not used
type FakePDMemberManager struct {
	err error
}

func NewFakePDMemberManager() *FakePDMemberManager {
	return &FakePDMemberManager{}
}

func (m *FakePDMemberManager) SetSyncError(err error) {
	m.err = err
}

func (m *FakePDMemberManager) Sync(tc *v1alpha1.TidbCluster) error {
	if m.err != nil {
		return m.err
	}
	if len(tc.Status.PD.Members) != 0 {
		// simulate status update
		tc.Status.ClusterID = string(uuid.NewUUID())
	}
	return nil
}
