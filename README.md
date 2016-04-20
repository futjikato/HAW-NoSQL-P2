# NoSQL WPP - Aufgabe 2

Zur Installation/Betrieb sind `redis`, `mongodb`, `git`, sowie `nodejs`, `npm` und `bower` notwendig.

Bower ist als npm Packet vorhanden und kann via `npm install -g bower` installiert werden.

## Installation 

```
git clone https://github.com/futjikato/HAW-NoSQL-P2.git
cd HAW-NoSQL-P2
npm install
bower install
```

## Betrieb

Ein Redis Server auf dem default port muss laufen.

Import der Daten: `DEBUG=* nodejs import.js --redis --mongo plz.data`
Leeren der DB und Import: `DEBUG=* nodejs import --redis --mongo -d plz.data`

Web-Interface starten: `DEBUG=* nodejs server.js`
Web-Interface ist verfügabr über Port `8080`

## Datenstruktur

### Redis

Jede PLZ ist ein Key. Als Wert ist eine Hash mit allen Daten hinterlegt. So kommt man über die PLZ an alle relevanten Daten wie den Namen und den Bundesstaat.

Zudem ist auch jeder Stadtname ein Key der auf die entsprechende PLZ verweist. Bei der Abfrage über den Namen wird also erst die PLZ geladen und darüber alle weiteren Informationen.

### Mongo DB

Datenstruktur aus plz.data wird 1:1 übernommen. Sprich: Es wird jede Zeile als Document in der Datenbank abgelegt.
