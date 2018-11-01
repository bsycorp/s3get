all: compile

clean:
	rm -rf build

compile: clean
	sh -c 'export GOOS=linux; export GOARCH=amd64; go get -d -t && go build -v -o build/s3get'