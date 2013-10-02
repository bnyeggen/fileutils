package fileutils

import (
	"os"
)

// Take the file at the given location, and return a slice of nSections line-
// aligned start byte positions.  This is kind of ad-hockey anyway, so I'm not
// particularly concerned about corner cases where nSections > lines, a split can't
// be found, etc. Intended for partitioning files for further processing.
// Currently this only works with UTF-8.
func GetLineSplits(infile string, nSections int64) []int64 {
	out := make([]int64, nSections)
	tempBuf := make([]byte, 4096)
	newline := byte(10) // := []byte("\n")

	fd, _ := os.Open(infile)
	defer fd.Close()
	fstat, _ := fd.Stat()
	size := fstat.Size()
	chunkSize := size / nSections

	//We could fire these off on different goroutines and synchronize with a waitgroup
	for chunk := int64(1); chunk < nSections; chunk++ {
		found := false
		for pos := chunk * chunkSize; !found && pos < (chunk+1)*chunkSize; pos += int64(len(tempBuf)) {
			fd.ReadAt(tempBuf, pos)
			for i, v := range tempBuf {
				if v == newline {
					out[chunk] = pos + int64(i)
					found = true
					break
				}
			}
		}
	}

	return out
}
