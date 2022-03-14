const { Kafka } = require('kafkajs')

const [,, topic = 'Logs', partition = 0] = process.argv

const createProducer = async () => {
  try {
    const kafka = new Kafka({
      clientId: 'kafka_ex_1',
      brokers: ['192.168.0.33:9092']
    })

    const producer = kafka.producer()
    console.log('Connecting to the Kafka Producer...')
    await producer.connect()
    console.log('Connected to the Kafka Producer!')

    const messageResult = await producer.send({
      topic,
      messages: [
        {
          value: 'Log message 1',
          partition
        }
      ]
    })
    console.log('Sent successfully:', JSON.stringify(messageResult))

    await producer.disconnect()
  } catch (err) {
    console.error(err.message)
  } finally {
    process.exit(0)
  }
}
createProducer()
