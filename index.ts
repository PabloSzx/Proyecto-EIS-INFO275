import csv from "csv-parser";
import fs from "fs";
import { isEqual, orderBy, remove, sortBy, toInteger, toNumber, uniqBy, uniqWith } from "lodash";
import xslx from "xlsx";
import path from "path";

const toDate = (date: string) => date.replace(/-/g, "/");

const readCSVFile = <T>(file: fs.PathLike): Promise<T[]> => {
  return new Promise<T[]>((resolve, reject) => {
    const results: T[] = [];
    fs.createReadStream(file)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", () => resolve(results))
      .on("error", reject);
  });
};

type OWIDRow = {
  iso_code: string;
  continent: string;
  location: string;
  date: string;
  total_cases: string;
  new_cases: string;
  new_cases_smoothed: string;
  total_deaths: string;
  new_deaths: string;
  new_deaths_smoothed: string;
  total_cases_per_million: string;
  new_cases_per_million: string;
  new_cases_smoothed_per_million: string;
  total_deaths_per_million: string;
  new_deaths_per_million: string;
  new_deaths_smoothed_per_million: string;
  new_tests: string;
  total_tests: string;
  total_tests_per_thousand: string;
  new_tests_per_thousand: string;
  new_tests_smoothed: string;
  new_tests_smoothed_per_thousand: string;
  tests_per_case: string;
  positive_rate: string;
  tests_units: string;
  stringency_index: string;
  population: string;
  population_density: string;
  median_age: string;
  aged_65_older: string;
  aged_70_older: string;
  gdp_per_capita: string;
  extreme_poverty: string;
  cardiovasc_death_rate: string;
  diabetes_prevalence: string;
  female_smokers: string;
  male_smokers: string;
  handwashing_facilities: string;
  hospital_beds_per_thousand: string;
  life_expectancy: string;
};

const OWIDData = readCSVFile<OWIDRow>("./data/owid/owid-covid-data.csv");

type Producto6Row = {
  Poblacion: string;
  "Casos Confirmados": string;
  Fecha: string;
  "Region ID": string;
  Region: string;
  "Provincia ID": string;
  Provincia: string;
  "Comuna ID": string;
  Comuna: string;
  Tasa: string;
};

const Producto6Data = readCSVFile<Producto6Row>("./data/producto6/bulk/data.csv");

type Producto7Row = {
  Region: string;
  "Codigo region": string;
  Poblacion: string;
  fecha: string;
  numero: string;
};

const Producto7Data = readCSVFile<Producto7Row>("./data/producto7/PCR_std.csv");

type Producto14Row = {
  Region: string;
  Fecha: string;
  Total: string;
};

const Producto14Data = readCSVFile<Producto14Row>("./data/producto14/FallecidosCumulativo_std.csv");

Promise.all([OWIDData, Producto6Data, Producto7Data, Producto14Data]).then(
  ([Internacional, CasosConfirmadosNacionales, PCRRealizadosNacionales, FallecidosNacionales]) => {
    remove(CasosConfirmadosNacionales, (v) => !v["Region ID"]);
    remove(CasosConfirmadosNacionales, (v) => !v.Poblacion);
    remove(PCRRealizadosNacionales, (v) => !v["Codigo region"]);
    remove(FallecidosNacionales, (v) => v.Region === "Total");
    remove(Internacional, (v) => v.location === "International" || v.location === "World");

    const regionesDirty = sortBy(
      uniqWith(
        [
          ...CasosConfirmadosNacionales.filter((v) => v["Region ID"]).map((v) => ({
            codigo: toInteger(v["Region ID"]),
            nombre: v.Region,
          })),
          ...PCRRealizadosNacionales.filter((v) => v["Codigo region"]).map((v) => ({
            codigo: toInteger(v["Codigo region"]),
            nombre: v.Region,
          })),
          ...FallecidosNacionales.filter((v) => v.Region).map((v) => ({
            codigo: toInteger(
              CasosConfirmadosNacionales.find((val) => val.Region === v.Region)?.["Region ID"] ??
                PCRRealizadosNacionales.find((val) => val.Region === v.Region)?.["Codigo region"] ??
                "-1"
            ),
            nombre: v.Region,
          })),
        ],
        isEqual
      ).filter((v) => v.codigo !== -1),
      (v) => v.codigo
    );

    const regionesClean = uniqBy(regionesDirty, (v) => v.codigo).map((v) => {
      return {
        ...v,
        poblacion: toInteger(
          PCRRealizadosNacionales.find((val) => toInteger(val["Codigo region"]) === v.codigo)
            ?.Poblacion ??
            (() => {
              throw Error("Población no pudo ser encontrada");
            })()
        ),
      };
    });
    const wb = xslx.utils.book_new();

    xslx.utils.book_append_sheet(wb, xslx.utils.json_to_sheet(regionesClean), "Regiones");

    const comunas = CasosConfirmadosNacionales.map((v) => ({
      codigo: toInteger(v["Comuna ID"]),
      nombre: v.Comuna,
      poblacion: toInteger(v.Poblacion),
      codigo_region: toInteger(v["Region ID"]),
    }));

    xslx.utils.book_append_sheet(wb, xslx.utils.json_to_sheet(comunas), "Comunas");

    const registroCasosConfirmadosNacionales = sortBy(
      CasosConfirmadosNacionales.map((v) => ({
        codigo_comuna: toInteger(v["Comuna ID"]),
        fecha: toDate(v.Fecha),
        cantidad: toInteger(v["Casos Confirmados"]),
      })),
      (v) => v.fecha
    );

    xslx.utils.book_append_sheet(
      wb,
      xslx.utils.json_to_sheet(registroCasosConfirmadosNacionales),
      "Casos Confirmados Nacionales"
    );

    const registroPCRRealizadosNacionales = sortBy(
      PCRRealizadosNacionales.map((v) => ({
        codigo_region: toInteger(v["Codigo region"]),
        fecha: toDate(v.fecha),
        cantidad: toInteger(v.numero),
      })),
      (v) => v.fecha
    );

    xslx.utils.book_append_sheet(
      wb,
      xslx.utils.json_to_sheet(registroPCRRealizadosNacionales),
      "PCR Nacionales"
    );

    const registroFallecidosAcumulativosNacionales = orderBy(
      FallecidosNacionales.map((v) => {
        return {
          codigo_region: toInteger(
            regionesDirty.find((val) => val.nombre === v.Region)?.codigo ??
              (() => {
                throw Error("Región no pudo ser encontrada para " + JSON.stringify(v));
              })()
          ),
          fecha: toDate(v.Fecha),
          cantidad: toInteger(v.Total),
        };
      }),
      ["codigo_region", "fecha"],
      ["asc", "asc"]
    );

    const registroFallecidosNacionales = registroFallecidosAcumulativosNacionales.map(
      (value, index) => {
        const previousValue = registroFallecidosAcumulativosNacionales[index - 1];
        if (previousValue?.codigo_region === value.codigo_region) {
          return {
            ...value,
            cantidad: value.cantidad - previousValue.cantidad,
          };
        }
        return value;
      }
    );

    xslx.utils.book_append_sheet(
      wb,
      xslx.utils.json_to_sheet(registroFallecidosNacionales),
      "Fallecidos Nacionales"
    );

    const continentes = uniqBy(Internacional, (v) => v.continent).map((v) => ({
      continente: v.continent,
    }));

    xslx.utils.book_append_sheet(wb, xslx.utils.json_to_sheet(continentes), "Continentes");

    const paises = uniqBy(Internacional, (v) => v.iso_code).map((v) => {
      return {
        codigo_iso: v.iso_code,
        nombre: v.location,
        continente: v.continent,
        poblacion: toInteger(v.population),
        densidad_poblacion: v.population_density
          ? toNumber(v.population_density)
          : (() => {
              console.log("no population density for " + v.location);
              return null;
            })(),
        mediana_edad: v.median_age
          ? toNumber(v.median_age)
          : (() => {
              console.log("no median age for " + v.location);
              return null;
            })(),
        gdp_per_capita: v.gdp_per_capita
          ? toNumber(v.gdp_per_capita)
          : (() => {
              console.log("no gdp_per_capita for " + v.location);
              return null;
            })(),
      };
    });

    xslx.utils.book_append_sheet(wb, xslx.utils.json_to_sheet(paises), "Paises");

    const registroFallecidosInternacionales = sortBy(
      Internacional.filter((v) => !!v.new_deaths).map((v) => {
        return {
          codigo_iso_pais: v.iso_code,
          fecha: toDate(v.date),
          cantidad: toInteger(v.new_deaths),
        };
      }),
      (v) => v.fecha
    );

    xslx.utils.book_append_sheet(
      wb,
      xslx.utils.json_to_sheet(registroFallecidosInternacionales, {
        cellDates: true,
      }),
      "Fallecidos Internacionales"
    );

    const registroPCRRealizadosInternacionales = sortBy(
      Internacional.filter((v) => !!v.new_tests).map((v) => {
        return {
          codigo_iso_pais: v.iso_code,
          fecha: toDate(v.date),
          cantidad: toInteger(v.new_tests),
        };
      }),
      (v) => v.fecha
    );

    xslx.utils.book_append_sheet(
      wb,
      xslx.utils.json_to_sheet(registroPCRRealizadosInternacionales),
      "PCR Internacionales"
    );

    const registroCasosConfirmadosInternacionales = sortBy(
      Internacional.filter((v) => !!v.new_cases).map((v) => {
        return {
          codigo_iso_pais: v.iso_code,
          fecha: toDate(v.date),
          cantidad: toInteger(v.new_cases),
        };
      }),
      (v) => v.fecha
    );

    xslx.utils.book_append_sheet(
      wb,
      xslx.utils.json_to_sheet(registroCasosConfirmadosInternacionales),
      "CasosConfirmados Internacional"
    );

    const newExcelLocation = path.resolve("./tables/data.xlsx");

    xslx.writeFile(wb, newExcelLocation);

    console.log("Done, XLSX Created at " + newExcelLocation);
  }
);
