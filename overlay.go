package overlayclient

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// OverlayClient routes requests to different underlying clients based on object type.
// It implements the full controller-runtime client.Client interface and could ideally be used
// as a drop-in replacement for a controller-runtime client.
type OverlayClient struct {
	// defaultClient handles all requests for object types not in the typeMap
	defaultClient client.Client

	// typeMap maps GroupVersionKind to specific clients
	typeMap map[schema.GroupVersionKind]client.Client

	// scheme is used for object type detection
	scheme *runtime.Scheme

	// TODO(jkyros): I doubt we'd be adding types on the fly ever, but just in case? So we never have to worry about it again?
	mutex sync.RWMutex
}

// OverlayClientOption configures the OverlayClient
type OverlayClientOption func(*OverlayClient)

// WithTypeMapping adds a mapping from a specific object type to a client
func WithTypeMapping(obj client.Object, targetClient client.Client) OverlayClientOption {
	return func(oc *OverlayClient) {
		// TODO(jkyros): I am worried about the scheme being set up right here since we are
		// multiplexing multiple clients, but most usage should be through WithGVKMapping not here?
		gvk, _, err := oc.scheme.ObjectKinds(obj)
		if err != nil || len(gvk) == 0 {
			// Fallback: try to get GVK from the object directly
			if gvkObj, ok := obj.(schema.ObjectKind); ok {
				if gvk := gvkObj.GroupVersionKind(); !gvk.Empty() {
					oc.typeMap[gvk] = targetClient
					return
				}
			}
			panic(fmt.Sprintf("failed to determine GVK for object type %T: %v", obj, err))
		}
		oc.typeMap[gvk[0]] = targetClient
	}
}

// WithGVKMapping adds a mapping from a GroupVersionKind to a client
func WithGVKMapping(gvk schema.GroupVersionKind, targetClient client.Client) OverlayClientOption {
	return func(oc *OverlayClient) {
		oc.typeMap[gvk] = targetClient
	}
}

// NewOverlayClient creates a new OverlayClient with the given default client and options
func NewOverlayClient(defaultClient client.Client, scheme *runtime.Scheme, opts ...OverlayClientOption) *OverlayClient {
	oc := &OverlayClient{
		defaultClient: defaultClient,
		typeMap:       make(map[schema.GroupVersionKind]client.Client),
		scheme:        scheme,
	}

	for _, opt := range opts {
		opt(oc)
	}

	return oc
}

// clientForObject determines which client to use for a given object. The precedence order should be
// the Object's explicit GVK (if it has one) > Scheme's GVK for the object (if it has one) > Reflection type
func (oc *OverlayClient) clientForObject(obj runtime.Object) client.Client {
	if obj == nil {
		return oc.defaultClient
	}

	gvk := obj.GetObjectKind().GroupVersionKind()

	// If GVK is not set on the object, try to infer it from the scheme
	if gvk.Empty() {
		gvks, _, err := oc.scheme.ObjectKinds(obj)
		if err == nil && len(gvks) > 0 {
			gvk = gvks[0]
		}
	}

	// If we still don't have a GVK, fall back to type-based lookup?
	// TODO(jkyros): Falling back to this might be a terrible idea, ideally
	// everything we have now would have an explicit GVK?
	if gvk.Empty() {
		objType := reflect.TypeOf(obj)
		if objType != nil {
			// Remove pointer indirection
			if objType.Kind() == reflect.Ptr {
				objType = objType.Elem()
			}

			// Try to find a matching client by type name
			oc.mutex.RLock()
			for mappedGVK, mappedClient := range oc.typeMap {
				if mappedGVK.Kind == objType.Name() {
					oc.mutex.RUnlock()
					return mappedClient
				}
			}
			oc.mutex.RUnlock()
		}

		return oc.defaultClient
	}

	oc.mutex.RLock()
	targetClient, exists := oc.typeMap[gvk]
	oc.mutex.RUnlock()

	if exists {
		return targetClient
	}

	return oc.defaultClient
}

// clientForObjectList determines which client to use for a given object list
// This is more complex because we need to infer the item type from the list type
func (oc *OverlayClient) clientForObjectList(obj client.ObjectList) client.Client {
	if obj == nil {
		return oc.defaultClient
	}

	// Try to get the GVK from the list object itself
	gvk := obj.GetObjectKind().GroupVersionKind()

	// If not available, try to infer from scheme
	if gvk.Empty() {
		gvks, _, err := oc.scheme.ObjectKinds(obj)
		if err == nil && len(gvks) > 0 {
			gvk = gvks[0]
		}
	}

	// Check if we have a direct mapping for the list type
	if !gvk.Empty() {
		oc.mutex.RLock()
		if targetClient, exists := oc.typeMap[gvk]; exists {
			oc.mutex.RUnlock()
			return targetClient
		}
		oc.mutex.RUnlock()

		// Try to map from list GVK to item GVK
		// e.g. NodeClaimList -> NodeClaim
		if gvk.Kind != "" {
			itemGVK := gvk
			if len(gvk.Kind) > 4 && gvk.Kind[len(gvk.Kind)-4:] == "List" {
				itemGVK.Kind = gvk.Kind[:len(gvk.Kind)-4]

				oc.mutex.RLock()
				if targetClient, exists := oc.typeMap[itemGVK]; exists {
					oc.mutex.RUnlock()
					return targetClient
				}
				oc.mutex.RUnlock()
			}
		}
	}

	return oc.defaultClient
}

// Get retrieves an obj for the given object key from the Kubernetes Cluster
func (oc *OverlayClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	targetClient := oc.clientForObject(obj)
	return targetClient.Get(ctx, key, obj, opts...)
}

// List retrieves list of objects for a given namespace and list options
func (oc *OverlayClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	targetClient := oc.clientForObjectList(list)
	return targetClient.List(ctx, list, opts...)
}

// Create saves the object obj in the Kubernetes cluster
func (oc *OverlayClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	targetClient := oc.clientForObject(obj)
	return targetClient.Create(ctx, obj, opts...)
}

// Delete deletes the given obj from Kubernetes cluster
func (oc *OverlayClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	targetClient := oc.clientForObject(obj)
	return targetClient.Delete(ctx, obj, opts...)
}

// Update updates the given obj in the Kubernetes cluster
func (oc *OverlayClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	targetClient := oc.clientForObject(obj)
	return targetClient.Update(ctx, obj, opts...)
}

// Patch patches the given obj in the Kubernetes cluster
func (oc *OverlayClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	targetClient := oc.clientForObject(obj)
	return targetClient.Patch(ctx, obj, patch, opts...)
}

// DeleteAllOf deletes all objects of the given type matching the given options
func (oc *OverlayClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	targetClient := oc.clientForObject(obj)
	return targetClient.DeleteAllOf(ctx, obj, opts...)
}

// Status returns a StatusWriter which can update status subresource
func (oc *OverlayClient) Status() client.StatusWriter {
	return &overlayStatusWriter{overlay: oc}
}

// SubResource returns a SubResourceClient for the given subresource
func (oc *OverlayClient) SubResource(subResource string) client.SubResourceClient {
	return &overlaySubResourceClient{
		overlay:     oc,
		subResource: subResource,
	}
}

// Scheme returns the scheme this client is using
func (oc *OverlayClient) Scheme() *runtime.Scheme {
	return oc.scheme
}

// RESTMapper returns the rest mapper this client is using
func (oc *OverlayClient) RESTMapper() meta.RESTMapper {
	// Return the default client's RESTMapper
	// This assumes all clients share compatible REST mapping
	return oc.defaultClient.RESTMapper()
}

// GroupVersionKindFor returns the GroupVersionKind for the given object
func (oc *OverlayClient) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return oc.defaultClient.GroupVersionKindFor(obj)
}

// IsObjectNamespaced returns true if the object is namespaced
func (oc *OverlayClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	return oc.defaultClient.IsObjectNamespaced(obj)
}

// overlayStatusWriter implements client.StatusWriter and routes to the appropriate client
type overlayStatusWriter struct {
	overlay *OverlayClient
}

func (osw *overlayStatusWriter) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	targetClient := osw.overlay.clientForObject(obj)
	return targetClient.Status().Create(ctx, obj, subResource, opts...)
}

func (osw *overlayStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	targetClient := osw.overlay.clientForObject(obj)
	return targetClient.Status().Update(ctx, obj, opts...)
}

func (osw *overlayStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	targetClient := osw.overlay.clientForObject(obj)
	return targetClient.Status().Patch(ctx, obj, patch, opts...)
}

// overlaySubResourceClient implements client.SubResourceClient and routes to the appropriate client
type overlaySubResourceClient struct {
	overlay     *OverlayClient
	subResource string
}

func (osrc *overlaySubResourceClient) Get(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceGetOption) error {
	targetClient := osrc.overlay.clientForObject(obj)
	return targetClient.SubResource(osrc.subResource).Get(ctx, obj, subResource, opts...)
}

func (osrc *overlaySubResourceClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	targetClient := osrc.overlay.clientForObject(obj)
	return targetClient.SubResource(osrc.subResource).Create(ctx, obj, subResource, opts...)
}

func (osrc *overlaySubResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	targetClient := osrc.overlay.clientForObject(obj)
	return targetClient.SubResource(osrc.subResource).Update(ctx, obj, opts...)
}

func (osrc *overlaySubResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	targetClient := osrc.overlay.clientForObject(obj)
	return targetClient.SubResource(osrc.subResource).Patch(ctx, obj, patch, opts...)
}
