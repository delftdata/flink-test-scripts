
consumer=$(./kafka/bin/kafka-console-consumer.sh \
--formatter 'kafka.coordinator.group.GroupMetadataManager$OffsetsMessageFormatter' \
--bootstrap-server $1 --topic __consumer_offsets --from-beginning \
--timeout-ms 2000 | grep benchmark-group | tr "[,]:" " " | awk '{print $1}' | tail -n1)

echo -e "PARTITION\tOFFSET\tTS" >> input-offsets
./kafka/bin/kafka-console-consumer.sh \
--formatter 'kafka.coordinator.group.GroupMetadataManager$OffsetsMessageFormatter' \
--bootstrap-server $1 --topic __consumer_offsets --from-beginning \
--timeout-ms 2000 | grep $consumer | tr "[,]:" " " | awk '{print $3 "\t" $5 "\t" $8}' >> input-offsets

python3 extract-input-throughtput.py
