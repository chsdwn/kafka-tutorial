const { Kafka } = require('kafkajs')

const createConsumer = async () => {
  try {
    const kafka = new Kafka({
      clientId: 'pub_sub_client',
      brokers: ['192.168.0.33:9092']
    })

    const consumer = kafka.consumer({
      groupId: 'pub_sub_consumer_group_1'
    })
    console.log('Connecting to the Kafka Consumer...')
    await consumer.connect()
    console.log('Connected to the Kafka Consumer!')

    console.log('Subscribing to the Kafka Consumer...')
    await consumer.subscribe({
      topic: 'pub_sub_topic',
      fromBeginning: true
    })
    console.log('Subscribed to the Kafka Consumer!')

    console.log('Running to the Kafka Consumer...')
    await consumer.run({
      eachMessage: async (result) => {
        const message = Buffer.from(result.message.value).toString()
        console.log('Message:', message, '- 1st consumer')
      }
    })
  } catch (err) {
    console.error(err.message)
  }
}
createConsumer()
