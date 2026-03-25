# db.py
from sqlalchemy import create_engine, Column, String, Integer, ForeignKey, DateTime, BigInteger, Index, text, UniqueConstraint, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/file_storage_db"
)

# PostgreSQL-specific engine configuration
engine = create_engine(
    DATABASE_URL,
    pool_size=20,
    max_overflow=40,
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_use_lifo=True,
    echo=False
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class File(Base):
    __tablename__ = "files"
    
    id = Column(String(36), primary_key=True, index=True)
    filename = Column(String(255), nullable=False, index=True)
    size = Column(BigInteger, default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Optional: Add signature for file similarity
    signature = Column(String(64), index=True, nullable=True)  # For grouping similar files
    
    # Relationships
    chunks = relationship("FileChunk", back_populates="file", cascade="all, delete-orphan")
    
    # Indexes
    __table_args__ = (
        Index('idx_files_filename_lower', text('LOWER(filename)')),
        Index('idx_files_created_at', created_at),
        Index('idx_files_size', size),
        Index('idx_files_signature', signature),
    )

class Chunk(Base):
    __tablename__ = "chunks"
    
    hash = Column(String(64), primary_key=True)  # SHA256 hash
    fast_hash = Column(String(16), index=True, nullable=True)  # xxhash for quick matching
    medium_hash = Column(String(32), index=True, nullable=True)  # blake2b for filtering
    path = Column(String(512), nullable=False)
    ref_count = Column(Integer, default=1, nullable=False, index=True)
    size = Column(Integer, default=0, nullable=False)
    compressed_size = Column(Integer, default=0, nullable=True)  # For future compression
    compression = Column(String(10), default="none", nullable=True)  # Compression algo
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Additional metadata for deduplication optimization
    chunk_type = Column(String(20), nullable=True)  # text, binary, compressed, etc.
    entropy = Column(Float, nullable=True)  # Shannon entropy of chunk
    compression_ratio = Column(Float, nullable=True)  # If compressed
    fingerprint = Column(String(32), index=True, nullable=True)  # MinHash or simhash
    
    # Relationships
    files = relationship("FileChunk", back_populates="chunk", cascade="all, delete-orphan")
    
    # Indexes for deduplication
    __table_args__ = (
        Index('idx_chunks_ref_count', ref_count),
        Index('idx_chunks_fast_hash', fast_hash),
        Index('idx_chunks_medium_hash', medium_hash),
        Index('idx_chunks_size', size),
        Index('idx_chunks_fingerprint', fingerprint),
        Index('idx_chunks_created_at', created_at),
        Index('idx_chunks_entropy', entropy),
        Index('idx_chunks_type', chunk_type),
    )

class FileChunk(Base):
    __tablename__ = "file_chunks"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    file_id = Column(String(36), ForeignKey("files.id", ondelete="CASCADE"), nullable=False, index=True)
    chunk_hash = Column(String(64), ForeignKey("chunks.hash", ondelete="CASCADE"), nullable=False, index=True)
    order_index = Column(Integer, nullable=False)
    
    # Relationships
    file = relationship("File", back_populates="chunks")
    chunk = relationship("Chunk", back_populates="files")
    
    # Composite index for faster lookups
    __table_args__ = (
        Index('idx_file_chunks_file_order', file_id, order_index),
        Index('idx_file_chunks_chunk_hash', chunk_hash),
        UniqueConstraint('file_id', 'order_index', name='uq_file_chunks_order'),
    )

# db.py - Fixed init_db function

def init_db():
    """Initialize database with proper schema and migrations"""
    
    # Create tables if they don't exist
    Base.metadata.create_all(bind=engine)
    
    # Add missing columns (migrations)
    if DATABASE_URL.startswith("postgresql"):
        with engine.connect() as conn:
            # Use AUTOCOMMIT for DDL operations
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            
            # Check if signature column exists in files table
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='files' AND column_name='signature'
            """))
            
            if not result.fetchone():
                print("Adding signature column to files table...")
                try:
                    conn.execute(text("ALTER TABLE files ADD COLUMN signature VARCHAR(64)"))
                    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_files_signature ON files(signature)"))
                    print("✓ Added signature column")
                except Exception as e:
                    print(f"⚠ Could not add signature column: {e}")
            
            # Check for other missing columns you might need
            # Check if compressed_size exists in chunks table
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name='chunks' AND column_name='compressed_size'
            """))
            
            if not result.fetchone():
                print("Adding compression columns to chunks table...")
                try:
                    conn.execute(text("ALTER TABLE chunks ADD COLUMN compressed_size INTEGER DEFAULT 0"))
                    conn.execute(text("ALTER TABLE chunks ADD COLUMN compression VARCHAR(10) DEFAULT 'none'"))
                    print("✓ Added compression columns")
                except Exception as e:
                    print(f"⚠ Could not add compression columns: {e}")
            
            conn.commit()
    
    elif "sqlite" in DATABASE_URL:
        with engine.connect() as conn:
            # SQLite doesn't support ALTER TABLE as well, so we need to recreate
            # For SQLite, it's easier to just drop and recreate
            try:
                # Check if column exists in SQLite
                result = conn.execute(text("PRAGMA table_info(files)"))
                columns = [row[1] for row in result]
                
                if 'signature' not in columns:
                    print("Adding signature column to files table (SQLite)...")
                    conn.execute(text("ALTER TABLE files ADD COLUMN signature VARCHAR(64)"))
                    conn.commit()
                    print("✓ Added signature column")
            except Exception as e:
                print(f"⚠ Could not add signature column: {e}")
                
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()