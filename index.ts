import { parse } from "json2csv";
import { promisify } from "util";
import fs from "fs";
import path from "path";
import { map, uniq } from "lodash";
import { Owid } from "./data/interfaces";

const readFile = promisify(fs.readFile);
const writeFile = promisify(fs.writeFile);

const dataRaw: Owid = JSON.parse(
  fs.readFileSync("./data/owid-covid-data.json", {
    encoding: "utf-8",
  })
);

const data = map(dataRaw, (value, key) => {
  return {
    ...value,
    ISO: key,
  };
});

console.log("writing...");
writeFile(
  path.resolve("./dataJSON.csv"),
  parse(data, {
    fields: uniq(data.flatMap((obj) => Object.keys(obj))),
  }),
  {
    encoding: "utf-8",
  }
).then(() => {
  console.log("done");
});
