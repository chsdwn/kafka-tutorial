const { Kafka } = require('kafkajs')

const createConsumer = async () => {
  try {
    const kafka = new Kafka({
      clientId: 'log_store_client',
      brokers: ['192.168.0.33:9092']
    })

    const consumer = kafka.consumer({
      groupId: 'log_store_consumer_group'
    })
    console.log('Connecting to the Kafka Consumer...')
    await consumer.connect()
    console.log('Connected to the Kafka Consumer!')

    console.log('Subscribing to the Kafka Consumer...')
    await consumer.subscribe({
      topic: 'log_store_topic',
      fromBeginning: true
    })
    console.log('Subscribed to the Kafka Consumer!')

    console.log('Running to the Kafka Consumer...')
    await consumer.run({
      eachMessage: async (result) => {
        const message = Buffer.from(result.message.value).toString()
        console.log('Message:', message, ' => Partition:', result.partition)
      }
    })
  } catch (err) {
    console.error(err.message)
  }
}
createConsumer()
