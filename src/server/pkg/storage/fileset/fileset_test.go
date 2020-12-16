package fileset

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"path"
	"testing"

	units "github.com/docker/go-units"
	"golang.org/x/sync/errgroup"

	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	"github.com/pachyderm/pachyderm/src/server/pkg/dbutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/chunk"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/fileset/index"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/track"
	"github.com/pachyderm/pachyderm/src/server/pkg/testutil"
)

const (
	max         = 20 * units.MB
	maxTags     = 10
	testPath    = "test"
	scratchPath = "scratch"
)

type testFile struct {
	path  string
	parts []*testPart
}

type testPart struct {
	tag  string
	data []byte
}

// type testDataOp struct {
// 	data   []byte
// 	tag    string
// 	hashes []string
// }

// func generateDataOps(n int) []*Metadata {
// 	numTags := rand.Intn(maxTags) + 1
// 	tags := []*chunk.Tag{}
// 	tagSize := n / numTags
// 	for i := 0; i < numTags-1; i++ {
// 		tags = append(tags, &chunk.Tag{
// 			Id:        strconv.Itoa(i),
// 			SizeBytes: int64(tagSize),
// 		})
// 	}
// 	tags = append(tags, &chunk.Tag{
// 		Id:        strconv.Itoa(numTags - 1),
// 		SizeBytes: int64(n - (numTags-1)*tagSize),
// 	})
// 	return tags
// }

func appendFile(t *testing.T, w *Writer, path string, parts []*testPart) {
	// Write content and tags.
	err := w.Append(path, func(fw *FileWriter) error {
		for _, part := range parts {
			fw.Append(part.tag)
			if _, err := fw.Write(part.data); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
}

func overwritePart(t *testing.T, w *Writer, path string, tag string) {
	panic("TODO")
}

func deletePart(t *testing.T, w *Writer, path string, tag string) {
	err := w.Delete(path, tag)
	require.NoError(t, err)
}

func writeFileSet(t *testing.T, s *Storage, fileSet string, files []*testFile) {
	w := s.NewWriter(context.Background(), fileSet)
	for _, file := range files {
		appendFile(t, w, file.path, file.parts)
	}
	require.NoError(t, w.Close())
}

func checkFile(t *testing.T, f File, tf *testFile) {
	r, w := io.Pipe()
	eg := errgroup.Group{}
	eg.Go(func() error {
		return f.Content(w)
	})
	eg.Go(func() (retErr error) {
		defer func() {
			if retErr != nil {
				r.CloseWithError(retErr)
			} else {
				r.Close()
			}
		}()
		for _, part := range tf.parts {
			actual := make([]byte, len(part.data))
			_, err := io.ReadFull(r, actual)
			if err != nil && err != io.EOF {
				return err
			}
		}
		return r.Close()
	})
	require.NoError(t, eg.Wait())
}

// func dataRefsToHashes(dataRefs []*chunk.DataRef) []string {
// 	var hashes []string
// 	for _, dataRef := range dataRefs {
// 		if dataRef.Hash == "" {
// 			hashes = append(hashes, dataRef.ChunkInfo.Chunk.Hash)
// 			continue
// 		}
// 		hashes = append(hashes, dataRef.Hash)
// 	}
// 	return hashes
// }

func newTestStorage(t *testing.T) *Storage {
	db := dbutil.NewTestDB(t)
	tr := track.NewTestTracker(t, db)
	return NewTestStorage(t, db, tr)
}

func TestWriteThenRead(t *testing.T) {
	ctx := testutil.NewTestContext(t)
	storage := newTestStorage(t)
	fileNames := index.Generate("abc")
	files := []*testFile{}
	for _, fileName := range fileNames {
		var parts []*testPart
		for _, tagInt := range rand.Perm(maxTags) {
			tag := fmt.Sprintf("%08x", tagInt)
			data := chunk.RandSeq(rand.Intn(max))
			parts = append(parts, &testPart{
				tag:  tag,
				data: data,
			})
		}
		files = append(files, &testFile{
			path: "/" + fileName,
		})
	}

	// Write out ten filesets where each subsequent fileset has the content of one random file changed.
	// Confirm that all of the content and hashes other than the changed file remain the same.
	fileset := path.Join(testPath, "0")

	// Write the files to the fileset.
	writeFileSet(t, storage, fileset, files)

	// Read the files from the fileset, checking against the recorded files.
	fs, err := storage.Open(ctx, []string{fileset})
	require.NoError(t, err)
	fileIter := files
	err = fs.Iterate(ctx, func(f File) error {
		tf := fileIter[0]
		fileIter = fileIter[1:]
		checkFile(t, f, tf)
		return nil
	})
	require.NoError(t, err)
}

// func TestCopy(t *testing.T) {
// 	fileSets := newTestStorage(t)
// 	msg := random.SeedRand()
// 	fileNames := index.Generate("abc")
// 	files := []*testFile{}
// 	// Write the initial fileset and count the chunks.
// 	for _, fileName := range fileNames {
// 		data := chunk.RandSeq(rand.Intn(max))
// 		files = append(files, &testFile{
// 			name: "/" + fileName,
// 			data: data,
// 			tags: generateTags(len(data)),
// 		})
// 	}
// 	originalPath := path.Join(testPath, "original")
// 	writeFileSet(t, fileSets, originalPath, files, msg)
// 	var initialChunkCount int64
// 	require.NoError(t, fileSets.ChunkStorage().List(context.Background(), func(_ string) error {
// 		initialChunkCount++
// 		return nil
// 	}), msg)
// 	// Copy intial fileset to a new copy fileset.
// 	r := fileSets.newReader(context.Background(), originalPath)
// 	copyPath := path.Join(testPath, "copy")
// 	wCopy := fileSets.newWriter(context.Background(), copyPath)
// 	require.NoError(t, r.iterate(func(fr *FileReader) error {
// 		return wCopy.CopyFile(fr)
// 	}), msg)
// 	require.NoError(t, wCopy.Close(), msg)
// 	// Compare initial fileset and copy fileset.
// 	rCopy := fileSets.newReader(context.Background(), copyPath)
// 	require.NoError(t, rCopy.iterate(func(fr *FileReader) error {
// 		checkFile(t, fr, files[0], msg)
// 		files = files[1:]
// 		return nil
// 	}), msg)
// 	// No new chunks should get created by the copy.
// 	var finalChunkCount int64
// 	require.NoError(t, fileSets.ChunkStorage().List(context.Background(), func(_ string) error {
// 		finalChunkCount++
// 		return nil
// 	}), msg)
// 	require.Equal(t, initialChunkCount, finalChunkCount, msg)
// }

// func TestCompaction(t *testing.T) {
// 	require.NoError(t, WithLocalStorage(func(fileSets *Storage) error {
// 		msg := random.SeedRand()
// 		numFileSets := 5
// 		// Generate filesets.
// 		files := generateFileSets(t, fileSets, numFileSets, testPath, msg)
// 		// Get the file hashes.
// 		getHashes(t, fileSets, files, msg)
// 		// Compact the files.
// 		_, err := fileSets.Compact(context.Background(), path.Join(testPath, Compacted), []string{testPath}, 0)
// 		require.NoError(t, err, msg)
// 		// Check the files.
// 		r := fileSets.NewReader(context.Background(), path.Join(testPath, Compacted))
// 		require.NoError(t, r.iterate(func(fr *FileReader) error {
// 			checkFile(t, fr, files[0], msg)
// 			files = files[1:]
// 			return nil
// 		}), msg)
// 		return nil
// 	}))
// }

// func generateFileSets(t *testing.T, fileSets *Storage, numFileSets int, prefix, msg string) []*testFile {
// 	fileNames := index.Generate("abcd")
// 	files := []*testFile{}
// 	// Generate the files and randomly distribute them across the filesets.
// 	var ws []*Writer
// 	for i := 0; i < numFileSets; i++ {
// 		ws = append(ws, fileSets.newWriter(context.Background(), path.Join(prefix, strconv.Itoa(i))))
// 	}
// 	for i, fileName := range fileNames {
// 		data := chunk.RandSeq(rand.Intn(max))
// 		files = append(files, &testFile{
// 			name: "/" + fileName,
// 			data: data,
// 			tags: generateTags(len(data)),
// 		})
// 		// Shallow copy for slicing as data is distributed.
// 		f := *files[i]
// 		wsCopy := make([]*Writer, len(ws))
// 		copy(wsCopy, ws)
// 		// Randomly distribute tagged data among filesets.
// 		for len(f.tags) > 0 {
// 			// Randomly select fileset to write to.
// 			i := rand.Intn(len(wsCopy))
// 			w := wsCopy[i]
// 			wsCopy = append(wsCopy[:i], wsCopy[i+1:]...)
// 			// Write the rest of the file if this is the last fileset.
// 			if len(wsCopy) == 0 {
// 				writeFile(t, w, &f, msg)
// 				break
// 			}
// 			// Choose a random number of the tags left.
// 			numTags := rand.Intn(len(f.tags)) + 1
// 			var size int
// 			for _, tag := range f.tags[:numTags] {
// 				size += int(tag.SizeBytes)
// 			}
// 			// Create file for writing and remove data/tags from rest of the file.
// 			fWrite := f
// 			fWrite.data = fWrite.data[:size]
// 			fWrite.tags = fWrite.tags[:numTags]
// 			f.data = f.data[size:]
// 			f.tags = f.tags[numTags:]
// 			writeFile(t, w, &fWrite, msg)
// 		}
// 	}
// 	for _, w := range ws {
// 		require.NoError(t, w.Close(), msg)
// 	}
// 	return files
// }

// func getHashes(t *testing.T, fileSets *Storage, files []*testFile, msg string) {
// 	writeFileSet(t, fileSets, path.Join(scratchPath, Compacted), files, msg)
// 	r := fileSets.newReader(context.Background(), path.Join(scratchPath, Compacted))
// 	require.NoError(t, r.iterate(func(fr *FileReader) error {
// 		files[0].hashes = dataRefsToHashes(fr.Index().DataOp.DataRefs)
// 		files = files[1:]
// 		return nil
// 	}), msg)
// }
