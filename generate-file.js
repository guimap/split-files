import { faker } from '@faker-js/faker'
import { Parser } from 'json2csv'
import { appendFileSync, writeFileSync } from 'fs'

function start () {
  const LINES_COUNT = 900 * 10**3
  // writeFileSync('big-file.csv', '')
  appendFileSync('big-file.csv', 'id,name\n')
  for (let i = 0; i < LINES_COUNT; i++) {
    appendFileSync('big-file.csv', `"${i+1}","${faker.name.findName()}"\n`)
  }
  console.log('Processo finalizado')

}

start()