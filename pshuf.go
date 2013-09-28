// Utility functions for processing files in parallel.  Static binaries are the
// Best Thing Ever.
package fileutils

import (
	"bufio"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Shuffle one file directly, outputting its lines to a channel, and synchronizing
// as appropriate
func shuffleOne(input string, out chan string, semaphore chan interface{}, wg *sync.WaitGroup) error {
	semaphore <- true
	lines := make([]string, 0, 1024)
	infile, _ := os.Open(input)
	scanner := bufio.NewScanner(bufio.NewReader(infile))
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	for _, v := range rand.New(rand.NewSource(time.Now().UTC().UnixNano())).Perm(len(lines)) {
		out <- lines[v]
	}
	infile.Close()
	wg.Done()
	<-semaphore
	return nil
}

// Shuffle the lines of the file in parallel by distributing the lines randomly
// to nTempFiles, shuffling those temp files in parallel (with a parallelism factor
// of nParShuffles), and concatenating them at the output file location.
func ShufflePar(input string, output string, nTempFiles int, nParShuffles int) error {
	rng := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	outchan := make(chan string, 1024)
	tempFileNames := make([]string, nTempFiles)
	tempFDs := make([]*os.File, nTempFiles)
	tempWriters := make([]*bufio.Writer, nTempFiles)
	for i := 0; i < nTempFiles; i++ {
		tempFile, _ := ioutil.TempFile("", "parShuffle")
		tempFileNames[i] = tempFile.Name()
		tempFDs[i] = tempFile
		tempWriters[i] = bufio.NewWriter(tempFile)
	}

	infile, err := os.Open(input)
	if err != nil {
		return err
	}
	outfile, err := os.Create(output)
	if err != nil {
		return err
	}
	outwriter := bufio.NewWriter(outfile)
	scanner := bufio.NewScanner(bufio.NewReader(infile))
	for scanner.Scan() {
		tempWriters[rng.Intn(len(tempWriters))].WriteString(scanner.Text() + "\n")
	}

	var wg sync.WaitGroup
	wg.Add(nTempFiles)
	semaphore := make(chan interface{}, nParShuffles)
	for i := 0; i < nTempFiles; i++ {
		tempWriters[i].Flush()
		tempFDs[i].Close()
		go shuffleOne(tempFileNames[i], outchan, semaphore, &wg)
	}

	go func() {
		wg.Wait()
		close(outchan)
	}()

	for s := range outchan {
		outwriter.WriteString(s + "\n")
	}

	outwriter.Flush()
	outfile.Close()
	for _, v := range tempFileNames {
		os.Remove(v)
	}
	return nil
}
