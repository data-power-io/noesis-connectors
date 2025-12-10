package tdp

// BOMRecord represents a row from the BOM manifest CSV
type BOMRecord struct {
	Level          int
	ParentPN       string
	PartNumber     string
	Revision       string
	Nomenclature   string
	Qty            int
	Type           string
	Material       string
	WeightKg       float64
	CAGECode       string
	CADFile        string
	LinkedDocument string
	DocumentType   string
}

// FileMetadata represents metadata about a file (CAD or document)
type FileMetadata struct {
	Path         string
	Name         string
	Size         int64
	ModifiedTime int64 // Unix timestamp in microseconds
	Extension    string
}

// EntityType represents the supported entity types
type EntityType string

const (
	EntityBOMManifest EntityType = "bom_manifest"
	EntityCADModels   EntityType = "cad_models"
	EntityDocuments   EntityType = "documents"
)

// SupportedEntities lists all supported entity types
var SupportedEntities = []EntityType{
	EntityBOMManifest,
	EntityCADModels,
	EntityDocuments,
}
