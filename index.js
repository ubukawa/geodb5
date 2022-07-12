// This is being edited.
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
const mbtilesDir = config.get('mbtilesDir')
const geojsonsDir = config.get('geojsonsDir')


let keyInProgress = []
let idle = true

const isIdle = () => {
    return idle
}

const fsOptions = {
    encoding: "utf8"
}

const sleep = (wait) => {
    return new Promise((resolve, reject) => {
        setTimeout( () => {resolve()}, wait)
    })
}


const queue = new Queue(async (t, cb) => {
    const startTime = new Date()
    const key = t.key
    const gjsPath = `${geojsonsDir}/inter-${key}.geojsons`
    const tmpPath = `${mbtilesDir}/part-${key}.mbtiles` //`${mbtilesDir}/part-${z}-${x}-${y}.mbtiles`
    const dstPath = `${mbtilesDir}/${key}.mbtiles` //`${mbtilesDir}/${z}-${x}-${y}.mbtiles`

    keyInProgress.push(key)
    console.log(`${keyInProgress} on process`)

    const FSstream = fs.createWriteStream(gjsPath, fsOptions)

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
        console.log(`FS write end: ${startTime.toISOString()} --> ${PendTime.toISOString()}`)
        //from here
        const VTconversion = new Promise((resolve, reject)=>{
            const tippecanoe = spawn(tippecanoePath, [
                `--output=${tmpPath}`,
                '--no-feature-limit',
                '--no-tile-size-limit',
                '--force',
                '--simplification=2',
                '--quiet',
                `--minimum-zoom=${minzoom}`,
                `--maximum-zoom=${maxzoom}`,
                gjsPath
                ]) 
               .on('exit', () => {
                    fs.renameSync(tmpPath, dstPath)
                    fs.unlinkSync(gjsPath)
                    const endTime = new Date()
                    console.log(`Tippecanoe ${endTime.toISOString()} (^o^)/`)
                    //keyInProgress = keyInProgress.filter((v) => !(v === key))
                    resolve()
                })
        })
        .then(()=> {
            const endTime = new Date()
            console.log(`${key} ends: ${startTime} --> ${endTime} (^o^)/`)
            keyInProgress = keyInProgress.filter((v) => !(v === key))
            return cb()
        })
        //until here    
    })

    const ogr2ogr = spawn(ogr2ogrPath, [
        '-f', 'GeoJSONSeq',
        '-lco', 'RS=YES',
        '/vsistdout/',
        //'-clipdst', 0, 52.4827, 5.625, 55.76573,
        //srcdb.url
        `small-data/${key}.geojson`
    ])
    
    //just in case (from here)
    while(!isIdle()){
        await sleep(3000)
    }
    //just in case (until here)

    ogr2ogr.stdout.pipe(parser)

 // The following part is moved into .then of
 //   const endTime = new Date()
 //   console.log(`${key} ends: ${startTime} --> ${endTime} (^o^)/`)
 //   keyInProgress = keyInProgress.filter((v) => !(v === key))
 //   return cb()

},{
    concurrent: 2,
    maxRetries: 3,
    retryDelay: 5000
})


const queueTasks = () => {
    for (let key of ['bndl1', 'bndl2', 'bndl3', 'bndl4', 'bndl5', 'bndl6']){
        queue.push({
            key: key
        })
    }
}

const shutdown = () => {
    console.log('shutdown (^_^)')
}

const main = async () =>{
    queueTasks()
    queue.on('drain', () => {
        shutdown()
    })
}

main()