rm -rf output

shopt -s extglob
rm !(bashrc|run.sh|upload.sh|AirlineSearchEngine.java)

cd airlines
rm !(airlines.csv)

cd ..
cd airports
rm !(airports.csv)

cd ..
cd routes
rm !(routes.csv)