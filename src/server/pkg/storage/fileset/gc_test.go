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
	tr := track.NewTestTracker(t, db)
	s := NewTestStorage(t, db, tr)
	gc := s.newGC()
	// create a file, which should have already expired.
	const testFilesetName = "testFilesetName"
	w := s.NewWriter(ctx, testFilesetName, WithTTL(-time.Hour))
	err := w.Append("a.txt", func(fw *FileWriter) error {
		fw.Append("tag1")
		_, err := fw.Write([]byte("test data"))
		return err
	})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	// check that it's there
	require.True(t, s.exists(ctx, testFilesetName))
	// run the gc
	countDeleted, err := gc.RunOnce(ctx)
	require.NoError(t, err)
	require.True(t, countDeleted > 0)
	// check that it's not there
}
