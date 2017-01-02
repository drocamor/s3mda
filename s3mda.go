package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	bucketName = flag.String("bucket", "rocamora-email", "Name of S3 bucket")
	prefix     = flag.String("prefix", "new", "Prefix to the objects in the S3 bucket")
	pageSize   = flag.Int64("page-size", 20, "Number of objects to show per page and download at once")
	delete     = flag.Bool("delete", true, "Delete objects after they are downloaded")
	profile    = flag.String("profile", "rocamora-email", "AWS profile to use")
	mailDir    = flag.String("maildir", "/Users/rocamora/Maildir", "Maildir to deliver to")
	svc        *s3.S3
)

func fetchMessage(obj *s3.Object, doneChan chan bool) {
	// get the name of the object without the prefix
	key := *obj.Key
	name := key[len(*prefix)+1:]

	fqdn, err := os.Hostname()
	if err != nil {
		log.Fatal("Can't get hostname: ", err)
	}

	hostname := strings.Split(fqdn, ".")[0]

	// Determine the maildir version of the name
	filename := fmt.Sprintf("%s/new/%d.%s.%s", *mailDir, obj.LastModified.Unix(), name, hostname)

	// get the object
	params := (&s3.GetObjectInput{}).
		SetBucket(*bucketName).
		SetKey(key)

	resp, err := svc.GetObject(params)

	if err != nil {
		log.Fatal("Could not get object: ", err)
	}

	// save the body of the object to the file
	f, err := os.Create(filename)
	if err != nil {
		log.Fatal("Error creating file: ", err)
	}

	defer f.Close()
	_, err = io.Copy(f, resp.Body)
	if err != nil {
		log.Fatal("Error writing file: ", err)
	}

	if *delete {
		log.Println("Will delete object")
		delParams := (&s3.DeleteObjectInput{}).
			SetBucket(*bucketName).
			SetKey(key)
		_, err := svc.DeleteObject(delParams)
		if err != nil {
			log.Fatal("Error deleting object: ", err)
		}
	}

	doneChan <- true

}

func processPage(page *s3.ListObjectsV2Output, lastPage bool) bool {
	log.Printf("Fetching %d messages", len(page.Contents))
	doneChan := make(chan bool)
	for _, obj := range page.Contents {
		go fetchMessage(obj, doneChan)
	}

	// Wait for all the messages to be fetched
	for i := 0; i < len(page.Contents); i++ {
		<-doneChan
	}

	return !lastPage
}

func main() {
	// Parse flags & conf file
	flag.Parse()

	sess, err := session.NewSessionWithOptions(session.Options{
		Profile:           *profile,
		SharedConfigState: session.SharedConfigEnable,
	})

	if err != nil {
		log.Fatal("Error creating AWS session: ", err)
	}
	svc = s3.New(sess)

	// List the contents of the bucket page by page with a smallish max number of objects (20)
	params := (&s3.ListObjectsV2Input{}).
		SetBucket(*bucketName).
		SetMaxKeys(*pageSize).
		SetPrefix(*prefix)

	err = svc.ListObjectsV2Pages(params, processPage)

	if err != nil {
		log.Fatal("Error: ", err)
	}

	// in a go routine for each object
	// Determine it's new name
	// Download it
	// delete the object (if this is set)
	// send bool to the done chan

	// recieve for the number of objects in that set

}
