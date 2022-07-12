const config = require('config')
const fs = require('fs')
const Queue = require('better-queue')
const { spawn } = require('child_process')
const Parser = require('json-text-sequence').parser

const srcdb = config.get('srcdb')
const ogr2ogrPath = config.get('ogr2ogrPath')
const tippecanoePath = config.get('tippecanoePath')
const minzoom = config.get('minzoom')
const maxzoom = config.get('maxzoom')

let keyInProgress = []
let idle = true

const isIdle = () => {
    return idle
}

const fsOptions = {
    encoding: "utf8"
}










const FSstream = fs.createWriteStream('testFS-bndl.geojsons', fsOptions)
//const downstream = process.stdout


function VTconversion(src){ // src is the source of the geojsons file.
    return new Promise((resolve, reject)=>{
        const startTime = new Date()
        const tippecanoe = spawn(tippecanoePath, [
            `--output=test1234.mbtiles`,
            '--no-feature-limit',
            '--no-tile-size-limit',
            '--force',
            '--simplification=2',
            '--quiet',
            `--minimum-zoom=1`,
            `--maximum-zoom=4`,
            src
            ]) 
           .on('exit', () => {
                //fs.renameSync(tmpPath, dstPath)
                //fs.unlinkSync(srcPath)
                const endTime = new Date()
                console.log(`Tippecanoe:${startTime.toISOString()}--> ${endTime.toISOString()} (^o^)/`)
                //keyInProgress = keyInProgress.filter((v) => !(v === key))
                resolve()
            })
    })
}

async function vtc(src){
    try{
        await VTconversion(src)
        console.log('VT conversion ended')
    } catch (err) {
        console.log(err.message)
    }
}


const parser = new Parser()
.on('data', f => {
    f.tippecanoe = {
        layer: srcdb.layer,
        minzoom: srcdb.minzoom,
        maxzoom: srcdb.maxzoom
    }
    FSstream.write(`\x1e${JSON.stringify(f)}\n`)
})
.on('finish', () => {
    FSstream.end()
    const PendTime = new Date()
    console.log(`FS write end: ${PendTime.toISOString()}`)
    //from here
    vtc('testFS-bndl.geojsons') //vector tile creation
    //until here    
})

const ogr2ogr = spawn(ogr2ogrPath, [
    '-f', 'GeoJSONSeq',
    '-lco', 'RS=YES',
    '/vsistdout/',
    //'-clipdst', 0, 52.4827, 5.625, 55.76573,
    //srcdb.url
    `small-data/bndl1.geojson`
])

ogr2ogr.stdout.pipe(parser)


