# docs.py - Centralized API documentation

API_TITLE = "File Storage with Deduplication API"
API_VERSION = "1.0.0"
API_DESCRIPTION = """
## 📁 File Storage with Intelligent Deduplication

A production-ready content-addressable storage system with automatic chunk-level deduplication.

### 🚀 Key Features
- **Automatic Deduplication**: Identical chunks stored once across all files
- **Async Processing**: Background processing with Redis Queue
- **Streaming Downloads**: Memory-efficient file reconstruction
- **Image Gallery**: Built-in gallery with thumbnails
- **Safe Deletion**: Preserves chunks used by other files
- **Comprehensive Stats**: Track storage efficiency

### 🏗️ Architecture
- **FastAPI**: High-performance async API
- **PostgreSQL**: Metadata and reference counting
- **Redis + RQ**: Job queue and background workers
- **Chunk Storage**: SHA256-based sharded directories

### 📊 Deduplication Process
1. Upload → Temp storage → Queue
2. Worker splits file into chunks
3. SHA256 hash identifies duplicates
4. Only unique chunks stored; ref_count incremented
5. Files reconstructed by chunk order
"""

API_TAGS = [
    {
        "name": "Files",
        "description": "Upload, download, list, search, delete files"
    },
    {
        "name": "Images", 
        "description": "Display, gallery, thumbnails for images"
    },
    {
        "name": "Statistics",
        "description": "Deduplication metrics and storage efficiency"
    },
    {
        "name": "Chunks",
        "description": "Chunk-level operations and cleanup"
    },
    {
        "name": "System",
        "description": "Health checks and monitoring"
    }
]

# Endpoint documentation templates
ENDPOINT_DOCS = {
    "upload": {
        "summary": "Upload a file",
        "description": """
        Upload file → queued for deduplication.
        - **<50MB**: 'uploads' queue (1hr timeout)
        - **>50MB**: 'large-files' queue (2hr timeout)
        Returns file_id and job_id immediately.
        """,
        "response_description": "File accepted, processing in background"
    },
    "list_files": {
        "summary": "List all files",
        "description": """
        Paginated file list with chunk counts.
        - `skip`: Pagination offset
        - `limit`: Items per page (1-1000)
        - `sort_by`: created_at, filename, size
        - `order`: asc, desc
        """,
        "response_description": "List of files with metadata"
    },
    "search_files": {
        "summary": "Search files",
        "description": """
        Case-insensitive search by filename and size range.
        - `q`: Search term (partial matches)
        - `min_size`/`max_size`: Size filter in bytes
        """,
        "response_description": "Search results"
    },
    "download": {
        "summary": "Download a file",
        "description": """
        Stream file reconstructed from chunks.
        - Preserves original filename
        - Memory-efficient for large files
        - Sets Content-Length for progress bars
        """,
        "response_description": "The requested file"
    },
    "stats": {
        "summary": "System statistics",
        "description": """
        Deduplication metrics:
        - Files: count, total size
        - Chunks: unique, references, sharing
        - Storage: logical vs physical, savings
        """,
        "response_description": "System statistics"
    },
    "health": {
        "summary": "Health check",
        "description": "Checks Redis, PostgreSQL, disk space",
        "response_description": "Health status"
    },
    "image_display": {
        "summary": "Display image",
        "description": "Render image directly in browser. Supports PNG, JPG, GIF, WebP",
        "response_description": "Image file"
    },
    "gallery": {
        "summary": "Image gallery",
        "description": "HTML gallery with modal view and pagination",
        "response_description": "HTML page"
    },
    "thumbnail": {
        "summary": "Image thumbnail",
        "description": "Generate resized thumbnail. Cache 24 hours.",
        "response_description": "Thumbnail image"
    },
    "list_images": {
        "summary": "List images (JSON)",
        "description": "JSON list of all images with URLs",
        "response_description": "Image list"
    },
    "delete_file": {
        "summary": "Delete file",
        "description": """
        Safe deletion:
        - Only removes chunks with ref_count=0
        - Shared chunks preserved
        - Requires `force=true` for files with shared chunks
        """,
        "response_description": "Deletion result"
    },
    "delete_batch": {
        "summary": "Delete multiple files",
        "description": "Batch delete with individual results",
        "response_description": "Batch deletion results"
    },
    "delete_preview": {
        "summary": "Preview deletion",
        "description": "Shows which files share chunks and what would be freed",
        "response_description": "Deletion preview"
    },
    "shared_chunks": {
        "summary": "Shared chunks analysis",
        "description": "Lists chunks this file shares with other files",
        "response_description": "Shared chunks details"
    },
    "unused_chunks": {
        "summary": "List unused chunks",
        "description": "Chunks with ref_count=0 ready for cleanup",
        "response_description": "Unused chunks list"
    },
    "cleanup_chunks": {
        "summary": "Cleanup unused chunks",
        "description": "Permanently delete chunks with ref_count=0",
        "response_description": "Cleanup results"
    }
}

# Response templates
RESPONSES = {
    "upload": {
        202: {"description": "File accepted, processing in background"},
        500: {"description": "Internal server error"}
    },
    "download": {
        200: {"description": "File downloaded successfully", "content": {"application/octet-stream": {}}},
        404: {"description": "File not found"},
        500: {"description": "Internal server error"}
    },
    "image": {
        200: {"description": "Image displayed successfully"},
        404: {"description": "File not found"},
        400: {"description": "File is not an image"}
    },
    "delete": {
        200: {"description": "File deleted successfully"},
        404: {"description": "File not found"},
        500: {"description": "Internal server error"}
    }
}