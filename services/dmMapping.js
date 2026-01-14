// services/dmMapping.js
// Mapping "logical fields" -> possible keywords found in APS flattened property keys.
// Used in Phase 1 extraction (APS -> MongoDB) and Phase 2 planner hints.

module.exports = {
  basic: {
    component_id: ['component_id', 'id', 'dbid', 'objectid', 'element id'],
    // IMPORTANT: we treat component_type as "CATEGORY" for chat filtering (Doors/Windows/...).
    // If your model stores OmniClass Title, we can map that here too.
    component_type: [
      'component_type', 'category', 'revit category', 'category name',
      'omniclass title', 'identity data.omniclass title'
    ],
    type_name: ['type name', 'type', 'identity data.type name', 'typename'],
    family_name: ['family name', 'family', 'identity data.family name', 'familyname'],
    is_asset: ['is_asset', 'asset', 'is facility asset', 'facility asset']
  },
  location: {
    level_number: ['level_number', 'level', 'base level', 'reference level', 'level name', 'story'],
    room_type: ['room type', 'space type'],
    room_name: ['room_name', 'room', 'room name', 'space', 'space name']
  },
  system: {
    system_type: ['system_type', 'system type'],
    system_name: ['system_name', 'system name', 'system']
  },
  equipment: {
    manufacturer: ['manufacturer', 'mfr', 'make'],
    model_name: ['model_name', 'model', 'model name'],
    specification: ['specification', 'spec', 'description', 'comments']
  },
  omniclass: {
    title: ['omniclass title', 'title', 'omniclass'],
    number: ['omniclass number', 'number', 'omniclass no', 'omniclass code']
  }
};
