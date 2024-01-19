/*
 * Copyright 2024 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package compress

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"

	"github.com/golang/snappy"
)

// float64ToByte converts a slice of float64 to a slice of byte
func float64ToByte(data []float64) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, data)
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}

// byteToFloat64 converts a slice of byte to a slice of float64
func byteToFloat64(data []byte) []float64 {
	buf := bytes.NewReader(data)
	var result []float64
	err := binary.Read(buf, binary.LittleEndian, &result)
	if err != nil {
		log.Fatal(err)
	}
	return result
}

func main() {
	// create a slice of float64
	data := []float64{3.14, 3.14, 3.15, 3.16, 3.17}
	fmt.Println("Original data:", data)

	// convert it to []byte
	dataByte := float64ToByte(data)
	fmt.Println("Data as []byte:", dataByte)

	// compress it using snappy
	compressed := snappy.Encode(nil, dataByte)
	fmt.Println("Compressed data:", compressed)

	// decompress it using snappy
	decompressed, err := snappy.Decode(nil, compressed)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Decompressed data:", decompressed)

	// convert it back to []float64
	dataFloat64 := byteToFloat64(decompressed)
	fmt.Println("Data as []float64:", dataFloat64)
}
