package badger

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"os"
	"time"
	"xbitman/kvstore/kv"
)

// Store implements bleve store
type Store struct {
	path string
	db   *badger.DB
	// mergeOperator kvstore.MergeOperator
}

// New .
func New(config map[string]interface{}) (*Store, error) {
	path, ok := config["path"].(string)
	if !ok {
		return nil, os.ErrInvalid
	}
	if path == "" {
		return nil, os.ErrInvalid
	}

	opt := badger.DefaultOptions(path)
	opt.ReadOnly = false
	opt.Compression = options.Snappy
	opt.WithLoggingLevel(badger.WARNING)

	// BdodbConfig
	if bdodbConfig, ok := config["BdodbConfig"].(Config); ok {
		opt.EncryptionKey = bdodbConfig.EncryptionKey
		opt.Logger = bdodbConfig.Logger
	} else {
		if bdodbConfig, ok := config["BdodbConfig"].(*Config); ok {
			opt.EncryptionKey = bdodbConfig.EncryptionKey
			opt.Logger = bdodbConfig.Logger
		}
	}
	/* usually modified options */

	// SyncWrites
	if SyncWrites, ok := config["SyncWrites"].(bool); ok {
		opt.SyncWrites = SyncWrites
	}
	// NumVersionsToKeep
	if NumVersionsToKeep, ok := config["NumVersionsToKeep"].(int); ok {
		opt.NumVersionsToKeep = NumVersionsToKeep
	}
	// ReadOnly
	if ReadOnly, ok := config["ReadOnly"].(bool); ok {
		opt.ReadOnly = ReadOnly
	}

	// Compression
	if Compression, ok := config["Compression"].(options.CompressionType); ok {
		opt.Compression = Compression
	}

	// InMemory
	if InMemory, ok := config["InMemory"].(bool); ok {
		opt.InMemory = InMemory
	}

	/* encryption related options */
	// EncryptionKeyRotationDuration
	if EncryptionKeyRotationDuration, ok := config["EncryptionKeyRotationDuration"].(time.Duration); ok {
		opt.EncryptionKeyRotationDuration = EncryptionKeyRotationDuration
	}

	/* fine tuning options */

	// LevelSizeMultiplier
	if LevelSizeMultiplier, ok := config["LevelSizeMultiplier"].(int); ok {
		opt.LevelSizeMultiplier = LevelSizeMultiplier
	}
	// MaxLevels
	if MaxLevels, ok := config["MaxLevels"].(int); ok {
		opt.MaxLevels = MaxLevels
	}
	// ValueThreshold
	if ValueThreshold, ok := config["ValueThreshold"].(int); ok {
		opt.ValueThreshold = ValueThreshold
	}
	// NumMemtables
	if NumMemtables, ok := config["NumMemtables"].(int); ok {
		opt.NumMemtables = NumMemtables
	}
	// BlockSize
	if BlockSize, ok := config["BlockSize"].(int); ok {
		opt.BlockSize = BlockSize
	}
	// BloomFalsePositive
	if BloomFalsePositive, ok := config["BloomFalsePositive"].(float64); ok {
		opt.BloomFalsePositive = BloomFalsePositive
	}

	// NumLevelZeroTables
	if NumLevelZeroTables, ok := config["NumLevelZeroTables"].(int); ok {
		opt.NumLevelZeroTables = NumLevelZeroTables
	}
	// NumLevelZeroTablesStall
	if NumLevelZeroTablesStall, ok := config["NumLevelZeroTablesStall"].(int); ok {
		opt.NumLevelZeroTablesStall = NumLevelZeroTablesStall
	}
	// ValueLogFileSize
	if ValueLogFileSize, ok := config["ValueLogFileSize"].(int64); ok {
		opt.ValueLogFileSize = ValueLogFileSize
	}
	// ValueLogMaxEntries
	if ValueLogMaxEntries, ok := config["ValueLogMaxEntries"].(uint32); ok {
		opt.ValueLogMaxEntries = ValueLogMaxEntries
	}

	// NumCompactors
	if NumCompactors, ok := config["NumCompactors"].(int); ok {
		opt.NumCompactors = NumCompactors
	}
	// CompactL0OnClose
	if CompactL0OnClose, ok := config["CompactL0OnClose"].(bool); ok {
		opt.CompactL0OnClose = CompactL0OnClose
	}

	// ZSTDCompressionLevel
	if ZSTDCompressionLevel, ok := config["ZSTDCompressionLevel"].(int); ok {
		opt.ZSTDCompressionLevel = ZSTDCompressionLevel
	}

	// VerifyValueChecksum
	if VerifyValueChecksum, ok := config["VerifyValueChecksum"].(bool); ok {
		opt.VerifyValueChecksum = VerifyValueChecksum
	}
	// ChecksumVerificationMode
	if ChecksumVerificationMode, ok := config["ChecksumVerificationMode"].(options.ChecksumVerificationMode); ok {
		opt.ChecksumVerificationMode = ChecksumVerificationMode
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.MkdirAll(path, os.FileMode(0700))
		if err != nil {
			return nil, err
		}
	}

	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}

	rv := Store{
		path: path,
		db:   db,
		// mergeOperator: mergeOperator,
	}
	return &rv, nil
}

// Close cleanup and close the current store
func (store *Store) Close() error {
	return store.db.Close()
}

// Reader initialize a new store.Reader
func (store *Store) Reader() (kv.Reader, error) {
	return &Reader{
		iteratorOptions: badger.DefaultIteratorOptions,
		store:           store,
	}, nil
}

// Writer initialize a new store.Writer
func (store *Store) Writer() (kv.Writer, error) {
	return &Writer{
		store: store,
	}, nil
}
