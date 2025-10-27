echo "Type the year to process (e.g., 2016):"
read year
echo "Type the beginning month (e.g., 1 for January):"
read start_month
echo "Type the ending month (e.g., 4 for April):"
read end_month

for i in $(seq $start_month $end_month); do
    echo "Processing file: ${i}-${year}.txt"
    python3 uol_news_extraction.py -q --path "${i}-${year}.txt"
done