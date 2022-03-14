const { Kafka } = require('kafkajs')

const createProducer = async () => {
  try {
    const kafka = new Kafka({
      clientId: 'pub_sub_client',
      brokers: ['192.168.0.33:9092']
    })

    const producer = kafka.producer()
    console.log('Connecting to the Kafka Producer...')
    await producer.connect()
    console.log('Connected to the Kafka Producer!')

    const messageResult = await producer.send({
      topic: 'pub_sub_topic',
      messages: [{
        value: 'New video',
        partition: 0
      }]
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
