
if [ -f "requirements.in" ]; then
    echo "File requirements.in exists."
else
    pip-compile --output-file requirements.in requirements.txt
fi

python src/worker.py 