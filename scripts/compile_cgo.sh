sed -i 's/^  # product-type = ""/  product-type = "logkeeper"/g' config/openGemini.conf

grep -rn "huawei.com/logkeeper/utils" go.mod
if [ $? == 1 ]
then
    sed "21 aimport \"huawei.com/logkeeper/utils/tokenizer\"" -i lib/tokenizer/tokenizer_c.go
    echo "require huawei.com/logkeeper/utils v1.3.5" >> go.mod
fi

go mod tidy

which python >/dev/null 2>&1
if [ $? == 0 ]
then
    python build.py --build-tags logstore
else
    python3 build.py --build-tags logstore
fi
