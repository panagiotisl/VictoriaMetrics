package storage

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkBlockStreamWriterBlocksWorstCase(b *testing.B) {
	benchmarkBlockStreamWriter(b, benchBlocksWorstCase, len(benchRawRowsWorstCase), false)
}

func BenchmarkBlockStreamWriterBlocksBestCase(b *testing.B) {
	benchmarkBlockStreamWriter(b, benchBlocksBestCase, len(benchRawRowsBestCase), false)
}

func BenchmarkBlockStreamWriterRowsWorstCase(b *testing.B) {
	benchmarkBlockStreamWriter(b, benchBlocksWorstCase, len(benchRawRowsWorstCase), true)
}

func BenchmarkBlockStreamWriterRowsBestCase(b *testing.B) {
	benchmarkBlockStreamWriter(b, benchBlocksBestCase, len(benchRawRowsBestCase), true)
}

func benchmarkBlockStreamWriter(b *testing.B, ebs []Block, rowsCount int, writeRows bool) {
	var rowsMerged uint64
	fmt.Printf("Rows: %d\n", rowsCount)
	b.ReportAllocs()
	b.SetBytes(int64(rowsCount))
	b.RunParallel(func(pb *testing.PB) {
		var bsw blockStreamWriter
		var mp inmemoryPart
		var ph partHeader
		var ebsCopy []Block
		for i := range ebs {
			var ebCopy Block
			ebCopy.CopyFrom(&ebs[i])
			ebsCopy = append(ebsCopy, ebCopy)
		}
		loopCount := 0
		for pb.Next() {
			if writeRows {
				for i := range ebsCopy {
					eb := &ebsCopy[i]
					if err := eb.UnmarshalData(); err != nil {
						panic(fmt.Errorf("cannot unmarshal block %d on loop %d: %w", i, loopCount, err))
					}
				}
			}

			bsw.InitFromInmemoryPart(&mp)
			for i := range ebsCopy {
				bsw.WriteExternalBlock(&ebsCopy[i], &ph, &rowsMerged, false)
			}
			bsw.MustClose()
			mp.Reset()
			loopCount++
		}
	})
}

var benchBlocksWorstCase = newBenchBlocks(benchRawRowsWorstCase)
var benchBlocksBestCase = newBenchBlocks(benchRawRowsBestCase)


func newBenchBlocks(rows []rawRow) []Block {
	var ebs []Block

	mp := newTestInmemoryPart(rows)
	var bsr blockStreamReader
	bsr.InitFromInmemoryPart(mp)
	for bsr.NextBlock() {
		var eb Block
		eb.CopyFrom(&bsr.Block)
		ebs = append(ebs, eb)
	}
	if err := bsr.Error(); err != nil {
		panic(fmt.Errorf("unexpected error when reading inmemoryPart: %w", err))
	}
	return ebs
}


var benchBlocksCityTemp = newBenchBlocks(benchRawRowsCityTemp)
var benchBlocksBaselTemp = newBenchBlocks(benchRawRowsBaselTemp)
var benchBlocksBaselWind = newBenchBlocks(benchRawRowsBaselWind)
var benchBlocksBitcoinPrice = newBenchBlocks(benchRawRowsBitcoinPrice)
var benchBlocksBirdMigration = newBenchBlocks(benchRawRowsBirdMigration)
var benchBlocksAirPressure = newBenchBlocks(benchRawRowsAirPressure)
var benchBlocksAirSensor = newBenchBlocks(benchRawRowsAirSensor)
var benchBlocksBioTemp = newBenchBlocks(benchRawRowsBioTemp)
var benchBlocksDewPointTemp = newBenchBlocks(benchRawRowsDewPointTemp)
var benchBlocksWindDir = newBenchBlocks(benchRawRowsWindDir)
var benchBlocksStocksDE = newBenchBlocks(benchRawRowsStocksDE)
var benchBlocksStocksUK = newBenchBlocks(benchRawRowsStocksUK)
var benchBlocksStocksUSA = newBenchBlocks(benchRawRowsStocksUSA)



func TestBlockStreamWriterRowsCityTemp(t *testing.T) {
	fmt.Println("CityTemp")
	testBlockStreamWriterSingle(benchBlocksCityTemp, len(benchRawRowsCityTemp), true)
}

func TestBlockStreamWriterRowsBaselTemp(t *testing.T) {
	fmt.Println("BaselTemp")
	testBlockStreamWriterSingle(benchBlocksBaselTemp, len(benchRawRowsBaselTemp), true)
}

func TestBlockStreamWriterRowsBaselWind(t *testing.T) {
	fmt.Println("BaselWind")
	testBlockStreamWriterSingle(benchBlocksBaselWind, len(benchRawRowsBaselWind), true)
}

func TestBlockStreamWriterRowsBitcoinPrice(t *testing.T) {
	fmt.Println("BitcoinPrice")
	testBlockStreamWriterSingle(benchBlocksBitcoinPrice, len(benchRawRowsBitcoinPrice), true)
}

func TestBlockStreamWriterRowsBirdMigration(t *testing.T) {
	fmt.Println("BirdMigration")
	testBlockStreamWriterSingle(benchBlocksBirdMigration, len(benchRawRowsBirdMigration), true)
}

func TestBlockStreamWriterRowsAirPressure(t *testing.T) {
	fmt.Println("AirPressure")
	testBlockStreamWriterSingle(benchBlocksAirPressure, len(benchRawRowsAirPressure), true)
}

func TestBlockStreamWriterRowsAirSensor(t *testing.T) {
	fmt.Println("AirSensor")
	testBlockStreamWriterSingle(benchBlocksAirSensor, len(benchRawRowsAirSensor), true)
}

func TestBlockStreamWriterRowsBioTemp(t *testing.T) {
	fmt.Println("BioTemp")
	testBlockStreamWriterSingle(benchBlocksBioTemp, len(benchRawRowsBioTemp), true)
}

func TestBlockStreamWriterRowsDewPointTemp(t *testing.T) {
	fmt.Println("DewPointTemp")
	testBlockStreamWriterSingle(benchBlocksDewPointTemp, len(benchRawRowsDewPointTemp), true)
}

func TestBlockStreamWriterRowsWindDir(t *testing.T) {
	fmt.Println("WindDir")
	testBlockStreamWriterSingle(benchBlocksWindDir, len(benchRawRowsWindDir), true)
}

func TestBlockStreamWriterStocksDE(t *testing.T) {
	fmt.Println("StocksDE")
	testBlockStreamWriterSingle(benchBlocksStocksDE, len(benchRawRowsStocksDE), true)
}

func TestBlockStreamWriterStocksUK(t *testing.T) {
	fmt.Println("StocksUK")
	testBlockStreamWriterSingle(benchBlocksStocksUK, len(benchRawRowsStocksUK), true)
}

func TestBlockStreamWriterStocksUSA(t *testing.T) {
	fmt.Println("StocksUSA")
	testBlockStreamWriterSingle(benchBlocksStocksUSA, len(benchRawRowsStocksUSA), true)
}

 /*
const layout = "01/02/2006 15:04:05"
const precisionBits = 64

func TestBlockStreamWriterRowsCityTemp(t *testing.T) {
	fmt.Println("CityTemp")

	var rows []rawRow
	var r rawRow
	totalBlocks := 0
	totalSize := uint64(0)
	totalEncodingTime := int64(0)
	f, err := os.Open("/home/panagiotis/timeseries/city_temperature-fixed.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		row4, err := strconv.Atoi(row[4])
		row5, err := strconv.Atoi(row[5])
		t, err := time.Parse(layout, fmt.Sprintf("%02d/%02d/%s 00:00:00", row4, row5, row[6]))
		if err != nil {
			fmt.Println(err)
		} else {
			if value, err := strconv.ParseFloat(row[7], 64); err == nil {
				r.TSID.MetricID = uint64(1)
				r.Timestamp = t.UnixNano()
				r.Value = value
				r.PrecisionBits = precisionBits
				rows = append(rows, r)
			}
		}
		if (len(rows) == maxRowsPerBlock) {
			var rowsMerged uint64
			var bsw blockStreamWriter
			var mp inmemoryPart
			var ph partHeader
			var ebsCopy []Block
			ebs := newBenchBlocks(rows)
			start := time.Now()
			for i := range ebs {
				var ebCopy Block
				ebCopy.CopyFrom(&ebs[i])
				ebsCopy = append(ebsCopy, ebCopy)
			}
			for i := range ebsCopy {
				eb := &ebsCopy[i]
				if err := eb.UnmarshalData(); err != nil {
					panic(fmt.Errorf("cannot unmarshal block %d: %w", i, err))
				}
			}
			bsw.InitFromInmemoryPart(&mp)
			for i := range ebsCopy {
				bsw.WriteExternalBlock(&ebsCopy[i], &ph, &rowsMerged, false)
			}
			encodingTime := time.Since(start)
			totalEncodingTime += encodingTime.Microseconds()
			totalBlocks++
			totalSize += bsw.valuesBlockOffset * 8

			bsw.MustClose()
			mp.Reset()
			rows = nil
		}
	}
	fmt.Printf("Bits/values: %f, Blocks: %d, Compression Time per Block: %f\n", float64(totalSize)/float64(totalBlocks * maxRowsPerBlock), totalBlocks,  float64(totalEncodingTime) / float64(totalBlocks))
}
*/


func testBlockStreamWriterSingle(ebs []Block, rowsCount int, writeRows bool) {
	var rowsMerged uint64
	var bsw blockStreamWriter
	var mp inmemoryPart
	var ph partHeader
	var ebsCopy []Block
	start := time.Now()
	for i := range ebs {
		var ebCopy Block
		ebCopy.CopyFrom(&ebs[i])
		ebsCopy = append(ebsCopy, ebCopy)
	}
	if writeRows {
		for i := range ebsCopy {
			eb := &ebsCopy[i]
			if err := eb.UnmarshalData(); err != nil {
				panic(fmt.Errorf("cannot unmarshal block %d: %w", i, err))
			}
		}
	}
	bsw.InitFromInmemoryPart(&mp)
	for i := range ebsCopy {
		bsw.WriteExternalBlock(&ebsCopy[i], &ph, &rowsMerged, false)
	}
	encodingTime := time.Since(start)
	blocks:= rowsCount / maxRowsPerBlock
	fmt.Printf("Bits/values: %f, Blocks: %d, Compression Time per Block: %f\n", float64(bsw.valuesBlockOffset * 8)/float64(rowsCount), blocks,  float64(encodingTime.Microseconds()) / float64(blocks))
	bsw.MustClose()
	mp.Reset()
}
