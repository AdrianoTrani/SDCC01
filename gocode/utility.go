// Various and useful scripts used by two or more types of nodes

package main

import (
	"fmt"
	"time"
	"runtime"
	"os"
	"bufio"
	"strings"
	"path/filepath"
)



// Global constants
const volumePath = "/mem"


// Force the container to stay active and idle for a certain amount of time
func shortSleep(){
	customPrintln("This container will wait for 20 seconds")
	time.Sleep(20 * time.Second)
}



// Print debug and resume information in a structured way
func customPrintln(info string){
	// Obtain information about the function
	pc, _, _, ok := runtime.Caller(1)
	if ok { 
		fn := runtime.FuncForPC(pc)

		// Print the information
		fmt.Println("[INFO] <",fn.Name(),"> <",time.Now().Format("15:04:05.000"),">",info)
	}
}

// Print a quick welcoming message
func greetings(){
	customPrintln("This container is ready to go")
}


//------------------------------------------------------------------------------------------------------------------------------
// Given a key, find the corresponding value and return it
// Return empty string if not found
func getValue(key string) (string){
	filePath := filepath.Join(volumePath, "mydata.txt")

	// Open the file to read-only mode, create the file if not present
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDONLY, 0600,)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	// Check every row in the Volume
	for scanner.Scan() {
		row := scanner.Text()
		customPrintln("Row: " + row)

		// Remove parenthesis (replace them with nothing)
		row = strings.Replace(row, "(", "", -1)
		row = strings.Replace(row, ")", "", -1)

		// Separate Key and Value
		parts := strings.SplitN(row, "," , 2)
		row_key := parts[0]
		row_value := parts[1]

		// Check the key
		if(row_key == key){
			customPrintln("Pair found")
			return row_value
		}
		

	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
	// Return void string if you didn't find it
	return ""
}

// Write a pair (Key, Value) inside the volume if not already present
func writeNewPair(key, value string) string{
	if(getValue(key) == ""){
		customPrintln("Wrote on volume: ("+ key + "," + value + ")\n")
		filePath := filepath.Join(volumePath, "mydata.txt")

		// Open the file to write-only mode, append to the end the provided pair, create the file if not present
		file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600,)
	
		if err != nil {
			panic(err)
		}

		defer file.Close()

		// Build the pair (Key,Value) in a specific format
		newRow := "(" + key + "," + value + ")\n"


		_, err = file.WriteString(newRow)
		if err != nil {
			panic(err)
		}

		return "Pair correctly added"

	// There is already a pair with this Key
	}else{
		return "Error: Key already present"
	}
	
}

// Read the entire content of the volume
func readAllVolume(){
	filePath := filepath.Join(volumePath, "mydata.txt")
	data, err := os.ReadFile(filePath)
	if err != nil {
		panic(err)
	}

	// Store content in a variable
	value := string(data)
	customPrintln("Volume contains: " + value)
}




//------------------------------------------------------------------------------------------------------------------------------
// Append a new line to the Operation Log
func appendOperation(opType, localTime, opInfo string){
	filePath := filepath.Join(volumePath, "opLog.txt")

	// Open the file to write-only mode, append to the end the provided pair, create the file if not present
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600,)

	if err != nil {
		panic(err)
	}

	defer file.Close()

	// Build the row in a specific format
	newRow := "[" + opType + "]<" + localTime + ">(" + opInfo + ")\n"

	_, err = file.WriteString(newRow)
	if err != nil {
		panic(err)
	}
	
}


// Read all Operation Log
func readAllOp() string{
	filePath := filepath.Join(volumePath, "opLog.txt")
	data, err := os.ReadFile(filePath)
	if err != nil {
		//panic(err)
		return ""
	}

	// Return content in a variable
	return string(data)
}

//------------------------------------------------------------------------------------------------------------------------------


