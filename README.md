# Data from liverc.com

## About

The script `sync` that spiders https://${YOUR_CLUB_HERE}.liverc.com/ and gets all the data into JSON files.

There will likely be another script to put all the results into a [https://www.sqlite.org/index.html](SQLite) database.

This may be interesting to some people...

## Dependencies

 * [pup](https://github.com/ericchiang/pup)
 * [jq](https://jqlang.github.io/jq/)
 * [jo](https://github.com/jpmens/jo)
 * [NodeJS](https://nodejs.org/en)
 * [GNU Parallel](https://www.gnu.org/software/parallel/)

And... a bunch of stuff that you already have

 * bash
 * awk
 * sed
 * curl
 * grep

## All results from your local club (in a large amount of JSON files)

To get all the results for a single club, into a directory full of JSON files, which you can then process, by other means...

```bash
./sync excelrc
```

You can replace excelrc with the domain of your local track which uses LiveRC

## Map all the tracks

Despite there being a table on the page only showing 100, it seems that there's many months of data about the tracks within that table and JS is just used to show 100.

```bash
curl https://live.liverc.com/ > tracks.html
```

### URLs to all the club pages

From that big HTML document, get a list of all the club pages

```bash
cat tracks.html  | ./pup 'table.track_list td:last-child a attr{href}' | uniq > tracks.txt
```

### Get the addresses

It seems there is an `<iframe>` on all the pages with a Google Maps embedded document. The liverc.com service does not seem to store geo or anything for the pages, but just queries Google with the address as written.

```bash
function doit {
    OUT="$(curl "$1" | ./pup 'iframe attr{src}' | sed 's/.*\;q=//')"
    echo "$2: $OUT"
}
export -f doit

cat tracks.txt | sed 's/\/$//' | awk '{ printf("https:%s\n%s\n", $1, substr($1, 3)); }' | parallel -N2 doit > track_addresses.txt
```

### Plot them

```bash
cat track_addresses.txt  | grep 'GB$' | sed 's/.*: //' | sed 's/+//'
```

Paste them into www.mapcustomizer.com or similar...


