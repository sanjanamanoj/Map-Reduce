DIR := $(shell basename `pwd`)

$(DIR).jar: *.java build.gradle Makefile
	gradle build
	gradle shadowJar
	cp build/libs/$(DIR)-all.jar $(DIR).jar

run: $(DIR).jar
	rm -rf output temp
	hadoop jar $(DIR).jar input output

clean:
	rm -rf build $(DIR).jar output temp .gradle
