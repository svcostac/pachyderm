package fileset

import (
	"context"
	"testing"
	"time"

	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	"github.com/pachyderm/pachyderm/src/server/pkg/dbutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/track"
)

func TestGC(t *testing.T) {
	ctx := context.Background()
	db := dbutil.NewTestDB(t)
	tr := track.NewPostgresTracker(db)
	s := NewTestStorage(t, db, tr)
	go s.GC(ctx)

	w := s.NewWriter(ctx, "testpath", WithTTL(-time.Microsecond))
	require.NoError(t, w.Append("a", func(fw *FileWriter) error {
		_, err := fw.Write([]byte("test data"))
		return err
	}))
}
