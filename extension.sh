rm -r output temp
mkdir -p bin
javac -cp "lib/*" ex-src/*.java -d bin
java -cp "lib/*:bin" W11Practical /cs/studres/CS1003/Practicals/W11/data/10_minutes output --sort
cat output/part-r-00000
