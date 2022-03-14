const { Kafka } = require('kafkajs')

const createTopic = async () => {
  try {
    const kafka = new Kafka({
      clientId: 'kafka_ex_1',
      brokers: ['192.168.0.33:9092']
    })

    const admin = kafka.admin()
    console.log('Connecting to the Kafka Broker...')
    await admin.connect()
    console.log('Connected to the Kafka Broker!')

    await admin.createTopics({
      topics: [
        {
          topic: 'Logs',
          numPartitions: 1
        },
        {
          topic: 'Logs2',
          numPartitions: 2
        }
      ]
    })
    console.log('Topic created successfully')
    await admin.disconnect()
  } catch (err) {
    console.error(err.message)
  } finally {
    process.exit(0)
  }
}
createTopic()
