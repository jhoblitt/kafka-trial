trialpb/trial.pb.go: trialpb/trial.proto

	cd trialpb && \
		protoc --go_out=. --go_opt=paths=source_relative  trial.proto
