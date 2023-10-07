const  avro = require('avsc');

const eventTypes = avro.Type.forSchema({
  type: 'record',
  fields: [
    {
      name: 'category',
      type: { type: 'enum', symbols: ['DOG', 'CAT'] }
    },
    {
      name: 'noise',
      type: 'string',
    }
  ]
});

module.exports = eventTypes;