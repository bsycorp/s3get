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
    "crypto/sha1"
    "crypto/sha256"
    "encoding/hex"
    "io"
    "path/filepath"
    "strconv"
    "crypto/tls"
    "net/http"
)

// Downloads an item from an S3 Bucket in the region configured in the shared config
// or AWS_REGION environment variable.
//
// Usage:
//    go run s3_download_object.go BUCKET ITEM
func main() {
    if len(os.Args) < 3 {
        exitErrorf("Bucket and item names required\nUsage: %s bucket_name item_name\n       %s bucket_name item_name sha256\n       %s bucket_name item_name version_id sha256",
            os.Args[0],os.Args[0],os.Args[0])
    }

    bucket := os.Args[1]
    item := os.Args[2]

    outputItem := filepath.Base(item)
    itemTmp := outputItem + ".unconfirmed"
    itemExpectedHash := ""
    if len(os.Args) == 4 {
        itemExpectedHash = os.Args[3]
    }

    versionId := ""
    if len(os.Args) == 5 {
        versionId = os.Args[3]
        itemExpectedHash = os.Args[4]
    }

    // setup getObject obj
    getObjectInput := &s3.GetObjectInput{
        Bucket: aws.String(bucket),
        Key:    aws.String(item),
    }

    if versionId != "" {
        getObjectInput = &s3.GetObjectInput{
            Bucket: aws.String(bucket),
            Key:    aws.String(item),
            VersionId:    aws.String(versionId),
        }
    }

    var fileTmp *os.File = nil
    var err error = nil
    logging := true

    if os.Args[len(os.Args) - 1] == "-" {
        //should write download file to stdout instead of disk
        fileTmp = os.Stdout
        logging = false

    } else {
        // open files for writing
        fileTmp, err = os.Create(itemTmp)
        if err != nil {
            exitErrorf("Unable to open file %q, %v", err)
        }
        defer fileTmp.Close()
    }

    awsRegion := "ap-southeast-2"
    if os.Getenv("AWS_REGION") != "" {
       awsRegion = os.Getenv("AWS_REGION")
    }

    awsConfig := &aws.Config{
        Region: aws.String(awsRegion),
    }

    //override http client with SSL ignoring tls config if specified
    if os.Getenv("AWS_NO_VERIFY_SSL") != "" {
        awsNoVerifySSL, _ := strconv.ParseBool(os.Getenv("AWS_NO_VERIFY_SSL"))
        if awsNoVerifySSL {
            defaultTransport := http.DefaultTransport.(*http.Transport)
            httpClientWithSelfSignedTLS := &http.Transport{
              Proxy:                 defaultTransport.Proxy,
              DialContext:           defaultTransport.DialContext,
              MaxIdleConns:          defaultTransport.MaxIdleConns,
              IdleConnTimeout:       defaultTransport.IdleConnTimeout,
              ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
              TLSHandshakeTimeout:   defaultTransport.TLSHandshakeTimeout,
              TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
            }
            awsConfig.WithHTTPClient(&http.Client{Transport: httpClientWithSelfSignedTLS})
        }
    }

    // Initialize a session in that the SDK will use to load credentials
    sess, _ := session.NewSession(awsConfig)

    downloader := s3manager.NewDownloader(sess)

    numBytes, err := downloader.Download(fileTmp, getObjectInput)
    if err != nil {
        exitErrorf("Unable to download item %q, %v", item, err)
    }

    tmpFilePath, err := filepath.Abs(itemTmp)
    if err != nil {
        exitErrorf("Failed to move file: %s", itemTmp)
    }
    filePath, err := filepath.Abs(outputItem)
    if err != nil {
        exitErrorf("Failed to move file: %s", outputItem)
    }

    if logging {
        fmt.Println("Downloaded: ", filePath, numBytes, "bytes")
    }

    //downloaded, if have has, check hash
    if itemExpectedHash != "" {
        fileHash := ""

        if len(itemExpectedHash) == 64 {
            h := sha256.New()
            if _, err := io.Copy(h, fileTmp); err != nil {
                exitErrorf("Error hashing file: %x", err)
            }
            fileHash = hex.EncodeToString(h.Sum(nil))

        } else if len(itemExpectedHash) == 40 {
            h := sha1.New()
            if _, err := io.Copy(h, fileTmp); err != nil {
                exitErrorf("Error hashing file: %x", err)
            }
            fileHash = hex.EncodeToString(h.Sum(nil))
        } else {
            exitErrorf("Invalid hash specified: %s", itemExpectedHash)
        }
        
        if fileHash == itemExpectedHash {
            if logging {
                fmt.Println("Downloaded hash is correct: ", fileHash)
            }
        } else {
            exitErrorf("Downloaded file hash failed: %s", fileHash)
        }
    }
    
    //move confirmed file
    err = os.Rename(tmpFilePath, filePath)
    if err != nil {
        exitErrorf("Failed to move file: %s to %s", tmpFilePath, filePath)
    }

    if logging {
        fmt.Println("Complete: ", filePath)
    }
}

func exitErrorf(msg string, args ...interface{}) {
    fmt.Fprintf(os.Stderr, msg+"\n", args...)
    os.Exit(1)
}
