package fileutils

import (
	"bufio"
	"container/heap"
	"io/ioutil"
	"os"
	"sort"
	"sync"
)

//Parallel merge-sort, using temporary files.

// Sort one file in memory, overwriting the original and synchronizing as appropriate.
// We could instead write to an output channel.
func sortOne(scratchfile string, semaphore chan interface{}, wg *sync.WaitGroup) {
	semaphore <- true
	lines := make([]string, 0, 1024)
	file, _ := os.Open(scratchfile)
	scanner := bufio.NewScanner(bufio.NewReader(file))
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	sort.Strings(lines)

	file.Close()
	file, _ = os.Create(scratchfile)
	writer := bufio.NewWriter(file)
	for _, line := range lines {
		writer.WriteString(line + "\n")
	}
	writer.Flush()
	file.Close()

	<-semaphore
	wg.Done()
}

// Represents a string that came from the reader at a particular index of a slice,
// so we can load another string from that reader
type stringPosPair struct {
	str string
	pos int
}

// A heap of stringPosPair
type stringPosPairHeap []stringPosPair

func (h stringPosPairHeap) Len() int           { return len(h) }
func (h stringPosPairHeap) Less(i, j int) bool { return h[i].str < h[j].str }
func (h stringPosPairHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *stringPosPairHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(stringPosPair))
}
func (h *stringPosPairHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Read from the given already-sorted individual files, and write their lines in
// totally sorted order to the output.
func merge(filenames []string, out string) error {
	nTempFiles := len(filenames)
	mergeheap := &stringPosPairHeap{}
	heap.Init(mergeheap)
	tempFDs := make([]*os.File, nTempFiles)
	tempScanners := make([]*bufio.Scanner, nTempFiles)

	for i := 0; i < nTempFiles; i++ {
		thisFD, err := os.Open(filenames[i])
		if err != nil {
			return err
		}
		tempFDs[i] = thisFD
		tempScanners[i] = bufio.NewScanner(bufio.NewReader(tempFDs[i]))
		if tempScanners[i].Scan() {
			heap.Push(mergeheap, stringPosPair{tempScanners[i].Text(), i})
		}
	}

	outfile, err := os.Create(out)
	if err != nil {
		return err
	}
	outwriter := bufio.NewWriter(outfile)

	for mergeheap.Len() != 0 {
		thisLowest := heap.Pop(mergeheap).(stringPosPair)
		thisPos := thisLowest.pos
		thisTxt := thisLowest.str
		if tempScanners[thisLowest.pos].Scan() {
			heap.Push(mergeheap, stringPosPair{tempScanners[thisPos].Text(), thisPos})
		}
		outwriter.WriteString(thisTxt + "\n")
	}
	outwriter.Flush()
	outfile.Close()
	for _, fd := range tempFDs {
		fd.Close()
	}
	return nil
}

// Sort the lines of input in lexiographic order, storing the result in output.
// Splits into nTempFiles chunks, of which nParSorts are sorted in memory
// simultaneously at any given time.  Eventually we may integrate a comparator.
func SortPar(input string, output string, nTempFiles int, nParSorts int) error {
	tempFileNames := make([]string, nTempFiles)
	tempFDs := make([]*os.File, nTempFiles)
	tempWriters := make([]*bufio.Writer, nTempFiles)
	for i := 0; i < nTempFiles; i++ {
		tempFile, err := ioutil.TempFile("", "parSort")
		if err != nil {
			return err
		}
		tempFileNames[i] = tempFile.Name()
		tempFDs[i] = tempFile
		tempWriters[i] = bufio.NewWriter(tempFile)
	}

	infile, err := os.Open(input)
	if err != nil {
		return err
	}
	inscanner := bufio.NewScanner(bufio.NewReader(infile))
	i := 0
	for inscanner.Scan() {
		//Writing round-robin like this might cause some file fragmentation
		//We could get more baroque with this by writing to a heap buffer that
		//does a running partial sort, but that may not help the underlying
		//golang sort routine.
		tempWriters[i%nTempFiles].WriteString(inscanner.Text() + "\n")
		i++
	}
	infile.Close()

	var wg sync.WaitGroup
	wg.Add(nTempFiles)
	semaphore := make(chan interface{}, nParSorts)
	for i := 0; i < nTempFiles; i++ {
		tempWriters[i].Flush()
		tempFDs[i].Close()
		go sortOne(tempFileNames[i], semaphore, &wg)
	}
	wg.Wait()
	return merge(tempFileNames, output)
}
