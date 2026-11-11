package measure

import (
	"blockEmulator/params"
	"encoding/csv"
	"log"
	"os"
)

// metricCSVStream is owned by the supervisor's serialized metric handler.
// It retains only csv.Writer's small buffer, not the experiment's history.
type metricCSVStream struct {
	file   *os.File
	writer *csv.Writer
	closed bool
}

func (s *metricCSVStream) open(name string, header []string) {
	if s.writer != nil || s.closed {
		return
	}
	dir := params.DataWrite_path + "supervisor_measureOutput/"
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		log.Panic(err)
	}
	f, err := os.OpenFile(dir+name+".csv", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Panic(err)
	}
	s.file, s.writer = f, csv.NewWriter(f)
	info, err := f.Stat()
	if err != nil {
		log.Panic(err)
	}
	if info.Size() == 0 {
		if err := s.writer.Write(header); err != nil {
			log.Panic(err)
		}
	}
}

func (s *metricCSVStream) write(name string, header, row []string) {
	s.open(name, header)
	if s.closed {
		log.Panic("write to closed metric stream: ", name)
	}
	if err := s.writer.Write(row); err != nil {
		log.Panic(err)
	}
}

func (s *metricCSVStream) flush() {
	if s.writer == nil {
		return
	}
	s.writer.Flush()
	if err := s.writer.Error(); err != nil {
		log.Panic(err)
	}
}

func (s *metricCSVStream) close() {
	if s.closed {
		return
	}
	s.flush()
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			log.Panic(err)
		}
	}
	s.file, s.writer, s.closed = nil, nil, true
}

func WriteMetricsToCSV(fileName string, colName []string, colVals [][]string) {
	// Construct directory path
	dirpath := params.DataWrite_path + "supervisor_measureOutput/"
	if err := os.MkdirAll(dirpath, os.ModePerm); err != nil {
		log.Panic(err)
	}

	// Construct target file path
	targetPath := dirpath + fileName + ".csv"

	// Open file, create if it does not exist
	file, err := os.OpenFile(targetPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	// Create CSV writer
	writer := csv.NewWriter(file)

	// Write header if the file is newly created
	fileInfo, err := file.Stat()
	if err != nil {
		log.Panic(err)
	}
	if fileInfo.Size() == 0 {
		if err := writer.Write(colName); err != nil {
			log.Panic(err)
		}
		writer.Flush()
	}

	// Write data
	for _, metricVal := range colVals {
		if err := writer.Write(metricVal); err != nil {
			log.Panic(err)
		}
		writer.Flush()
	}
}
