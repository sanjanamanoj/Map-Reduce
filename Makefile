Default:
	cd train && make
	cp train/train.jar .
	cd test && make
	cp test/test.jar .

run:
	make	
	rm -rf AnnaloganathanManojkumar.csv
	hadoop jar train.jar input output
	cp output/models/model* input/models/
	hadoop jar test.jar input output
	cat output/results/* > AnnaloganathanManojkumar.csv

clean:
	rm -rf output test.jar train.jar 
	cd train && make clean
	cd test && make clean

