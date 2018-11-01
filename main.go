/*
   Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.

   This file is licensed under the Apache License, Version 2.0 (the "License").
   You may not use this file except in compliance with the License. A copy of
   the License is located at

    http://aws.amazon.com/apache2.0/

   This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
   CONDITIONS OF ANY KIND, either express or implied. See the License for the
   specific language governing permissions and limitations under the License.
*/

package main

import (
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/service/s3/s3manager"

    "fmt"
    "os"
    "crypto/sha256"
    "encoding/hex"
    "io"
    "path/filepath"
)

// Downloads an item from an S3 Bucket in the region configured in the shared config
// or AWS_REGION environment variable.
//
// Usage:
//    go run s3_download_object.go BUCKET ITEM
func main() {
    if len(os.Args) < 3 {
        exitErrorf("Bucket and item names required\nUsage: %s bucket_name item_name",
            os.Args[0])
    }

    bucket := os.Args[1]
    item := os.Args[2]
    
    outputItem := filepath.Base(item)
    itemTmp := outputItem + ".unconfirmed"
    itemExpectedHash := ""
    if len(os.Args) > 3 {
        itemExpectedHash = os.Args[3]
    }
    
    file, err := os.Create(outputItem)
    if err != nil {
        exitErrorf("Unable to open file %q, %v", err)
    }
    fileTmp, err := os.Create(itemTmp)
    if err != nil {
        exitErrorf("Unable to open file %q, %v", err)
    }

    defer file.Close()

    awsRegion := "ap-southeast-2"
    if os.Getenv("AWS_REGION") != "" {
       awsRegion = os.Getenv("AWS_REGION")
    }

    // Initialize a session in that the SDK will use to load credentials
    sess, _ := session.NewSession(&aws.Config{
        Region: aws.String(awsRegion)},
    )

    downloader := s3manager.NewDownloader(sess)

    numBytes, err := downloader.Download(fileTmp,
        &s3.GetObjectInput{
            Bucket: aws.String(bucket),
            Key:    aws.String(item),
        })
    if err != nil {
        exitErrorf("Unable to download item %q, %v", item, err)
    }

    fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
    
    //downloaded, if have has, check hash
    if itemExpectedHash != "" {
        h := sha256.New()
        if _, err := io.Copy(h, fileTmp); err != nil {
            exitErrorf("Error hashing file: %x", err)
        }
        fileHash := hex.EncodeToString(h.Sum(nil))
        
        if fileHash == itemExpectedHash {
            fmt.Println("Downloaded hash is correct: ", fileHash)
        } else {
            exitErrorf("Downloaded file hash failed: %s", fileHash)
        }
    }
    
    //move confirmed file
    tmpFilePath, err := filepath.Abs(itemTmp)
    if err != nil {
        exitErrorf("Failed to move file: %s", itemTmp)
    }
    filePath, err := filepath.Abs(outputItem)
    if err != nil {
        exitErrorf("Failed to move file: %s", outputItem)
    }
    err = os.Rename(tmpFilePath, filePath)
    if err != nil {
        exitErrorf("Failed to move file: %s to %s", tmpFilePath, filePath)
    }
    fmt.Println("Complete: ", filePath)
}

func exitErrorf(msg string, args ...interface{}) {
    fmt.Fprintf(os.Stderr, msg+"\n", args...)
    os.Exit(1)
}


