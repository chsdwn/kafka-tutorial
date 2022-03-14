const { Kafka } = require('kafkajs')
const logs = require('./logs.json')

const createProducer = async () => {
  try {
    const kafka = new Kafka({
      clientId: 'log_store_client',
      brokers: ['192.168.0.33:9092']
    })

    const producer = kafka.producer()
    console.log('Connecting to the Kafka Producer...')
    await producer.connect()
    console.log('Connected to the Kafka Producer!')

    const messages = logs.map((log) => ({
      value: JSON.stringify(log),
      partition: log.type === 'system' ? 0 : 1
    }))
    const messageResult = await producer.send({
      topic: 'log_store_topic',
      messages
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
