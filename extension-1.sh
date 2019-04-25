rm -r output temp
mkdir -p bin
javac -cp "lib/*" ex-1-src/*.java -d bin
java -cp "lib/*:bin" W11Practical /cs/studres/CS1003/Practicals/W11/data/10_minutes output
cat output/part-r-00000
