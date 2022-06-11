package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rekognition"
)

// Pseudocode for Concurrent Rekognition Image Analysis

// Bucket name is defined as a const
// const bucketName string = "testbucketchinmay"

type Rekognitionresponse struct {
	Key    string
	Values []ImageResponse
}

type ImageResponse struct {
	Title     string
	Locations []LocationsType
}

type LocationsType struct {
	Left   float64
	Top    float64
	Height float64
	Width  float64
}

func main() {
	lambda.Start(HandleRequest)
}

// Gets called by main , event is implicit - with a stringified array of image locations
func HandleRequest(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	ApiResponse := events.APIGatewayProxyResponse{}

	if request.HTTPMethod == "OPTIONS" {
		ApiResponse = events.APIGatewayProxyResponse{StatusCode: 200, Headers: map[string]string{
			"Access-Control-Allow-Origin":  "*",
			"Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization",
			"Access-Control-Allow-Methods": "DELETE,GET,OPTIONS,POST,PUT",
			"Cache-Control":                "private, no-cache, no-store, max-age=0, must-revalidate",
		}}
		return ApiResponse, nil
	}

	// Switch for identifying the HTTP request

	if request.HTTPMethod == "POST" {
		//      err := fastjson.Validate(request.Body)

		var arr []string
		_ = json.Unmarshal([]byte(request.Body), &arr)

		fmt.Println("Converted incoming JSON Body to Go Array : ")
		for _, img := range arr {
			fmt.Println(img)
		}

		imageArr := processImages(arr)
		fmt.Println("Created array of image locations", imageArr)
		jsonString, err := json.Marshal(imageArr)
		if err != nil {
			fmt.Println("Got error during marhsall/stringify", err)
		} else {
			fmt.Println("Marshalled/stringified Array of results", jsonString)
			ApiResponse = events.APIGatewayProxyResponse{Body: string(jsonString), StatusCode: 200, Headers: map[string]string{
				"Access-Control-Allow-Origin":  "*",
				"Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization",
				"Access-Control-Allow-Methods": "DELETE,GET,OPTIONS,POST,PUT",
				"Cache-Control":                "private, no-cache, no-store, max-age=0, must-revalidate",
			}}
		}
	} else {
		ApiResponse = events.APIGatewayProxyResponse{Body: "invalid request", StatusCode: 400}
	}

	// Response
	return ApiResponse, nil
}

// Receives array of image locations , returns an array of results for the image
func processImages(images []string) []Rekognitionresponse {

	// Frontend sends us an array containing all image names
	// ["image1" , "image2" , "image3"]

	// 2 lines below create an array slice of length 0 and then populate it with image names
	// images:= make([]string , 0)
	// images= append(images , "chuttersnap-TSgwbumanuE-unsplash.jpg" , "business-team-meeting-boardroom.jpg" , "happy-dog-wears-flowers.jpg")
	// images= append(images , "chuttersnap-TSgwbumanuE-unsplash.jpg" )

	// Create 1 channel
	ch := make(chan *Rekognitionresponse, len(images))

	fmt.Println("length of images", len(images))

	// Run our func concurrently for every image in the array slice
	for _, img := range images {
		go getImageResults(img, ch)
	}

	fmt.Println("Waiting for goroutines to finish...")

	// Create results slice to store results of every image computation
	results := make([]Rekognitionresponse, len(images))

	// Collect results
	// Send every item from our channel to results array (will contain analysis results)
	for i := range results {
		result := <-ch // As values come in to channel they get assigned to results array
		results[i] = *result
	}

	// Above for loop is blocking -> below code will not execute until we are done reading ALL values from channel

	close(ch)
	fmt.Println("Done!")

	// Log Results
	fmt.Printf("Results: %+v\n", results)

	return results
}

// This function takes the image location and calculates labels and passes results to the channel
func getImageResults(s string, ch chan<- *Rekognitionresponse) {

	// Passes image name to getLabels and sends results of that to ch
	res, err := getLabels(s)
	fmt.Println("finished getlabels")
	fmt.Println(res, err)
	fmt.Println("starting sending to channel")

	result := Rekognitionresponse{Key: s}
	values := make([]ImageResponse, 0)

	for _, label := range res.Labels {
		instances := label.Instances
		if instances != nil {
			locations := make([]LocationsType, 0)
			for _, instance := range instances {
				location := LocationsType{
					Top:    *instance.BoundingBox.Top,
					Left:   *instance.BoundingBox.Left,
					Width:  *instance.BoundingBox.Width,
					Height: *instance.BoundingBox.Height,
				}

				locations = append(locations, location)
			}
			imageRes := ImageResponse{
				Title:     *label.Name,
				Locations: locations}

			values = append(values, imageRes)
		}
	}

	result.Values = values

	ch <- &result
	fmt.Println("finished sending to channel")

	// ch <- s
}

// Above function should receive the string , compute the results using rekognition API ,
// then send back to channel

// Rekognition SDK Calls

func getLabels(objName string) (*rekognition.DetectLabelsOutput, error) {
	svc := rekognition.New(session.New())

	bucketName := string(os.Getenv("BUCKET_NAME"))

	input := &rekognition.DetectLabelsInput{
		Image: &rekognition.Image{
			S3Object: &rekognition.S3Object{
				Bucket: aws.String(bucketName),
				Name:   aws.String(objName),
			},
		},
		MaxLabels:     aws.Int64(10),
		MinConfidence: aws.Float64(90.000000),
	}

	fmt.Println("calling detectLabels")

	result, err := svc.DetectLabels(input)
	fmt.Println("completed calling detectLabels")
	if err != nil {
		fmt.Println("got errors")
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case rekognition.ErrCodeInvalidS3ObjectException:
				fmt.Println(rekognition.ErrCodeInvalidS3ObjectException, aerr.Error())
			case rekognition.ErrCodeInvalidParameterException:
				fmt.Println(rekognition.ErrCodeInvalidParameterException, aerr.Error())
			case rekognition.ErrCodeImageTooLargeException:
				fmt.Println(rekognition.ErrCodeImageTooLargeException, aerr.Error())
			case rekognition.ErrCodeAccessDeniedException:
				fmt.Println(rekognition.ErrCodeAccessDeniedException, aerr.Error())
			case rekognition.ErrCodeInternalServerError:
				fmt.Println(rekognition.ErrCodeInternalServerError, aerr.Error())
			case rekognition.ErrCodeThrottlingException:
				fmt.Println(rekognition.ErrCodeThrottlingException, aerr.Error())
			case rekognition.ErrCodeProvisionedThroughputExceededException:
				fmt.Println(rekognition.ErrCodeProvisionedThroughputExceededException, aerr.Error())
			case rekognition.ErrCodeInvalidImageFormatException:
				fmt.Println(rekognition.ErrCodeInvalidImageFormatException, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		// return
	}
	return result, nil
}
