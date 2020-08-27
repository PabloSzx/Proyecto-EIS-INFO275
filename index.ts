import { groupBy, map, sumBy } from "lodash";
import path from "path";
import xslx from "xlsx";

import { getData } from "./getData";

const dataPromise = getData();

async function TasaFallecidosPoblacion() {
  const {
    regionesClean,
    paises,
    registroNuevosFallecidosInternacionales: registroFallecidosInternacionales,
    registroNuevosFallecidosNacionales: registroFallecidosNacionales,
  } = await dataPromise;

  const tasaNacional = registroFallecidosNacionales.map(({ cantidad, codigo_region, fecha }) => {
    const region =
      regionesClean.find((region) => {
        return region.codigo === codigo_region;
      }) ??
      (() => {
        throw Error("Región no pudo ser encontrada para " + codigo_region.toString());
      })();

    return {
      region: region.nombre,
      fecha,
      poblacion: region.poblacion,
      cantidad,
      tasa: cantidad / region.poblacion,
    };
  });

  const tasaInternacional = registroFallecidosInternacionales.map(
    ({ cantidad, codigo_iso_pais, fecha }) => {
      const pais =
        paises.find((pais) => {
          return pais.codigo_iso === codigo_iso_pais;
        }) ??
        (() => {
          throw Error("País no pudo ser encontrada para " + codigo_iso_pais.toString());
        })();

      return {
        pais: pais.nombre,
        fecha,
        poblacion: pais.poblacion,
        cantidad,
        tasa: cantidad / pais.poblacion,
      };
    }
  );

  return { tasaNacional, tasaInternacional };
}

async function TasaContagiadosPoblacion() {
  const {
    regionesClean,
    comunas,
    paises,
    registroNuevosCasosConfirmadosInternacionales,
    registroNuevosCasosConfirmadosNacionales,
  } = await dataPromise;

  const tasaComunalNacional = registroNuevosCasosConfirmadosNacionales.map(
    ({ cantidad, codigo_comuna, fecha }) => {
      const comuna =
        comunas.find((comuna) => {
          return comuna.codigo === codigo_comuna;
        }) ??
        (() => {
          throw Error("Comuna no pudo ser encontrada para " + codigo_comuna.toString());
        })();
      const region =
        regionesClean.find((region) => {
          return region.codigo === comuna.codigo_region;
        }) ??
        (() => {
          throw Error("Región no pudo ser encontrada para " + comuna.nombre);
        })();

      return {
        comuna: comuna.nombre,
        region: region.nombre,
        fecha,
        poblacionRegion: region.poblacion,
        poblacionComuna: comuna.poblacion,
        cantidad,
        tasa: cantidad / comuna.poblacion,
      };
    }
  );

  const tasaRegionalNacional = map(groupBy(tasaComunalNacional, "region"), (value, region) => {
    const agrupadosPorFecha = groupBy(value, "fecha");
    const poblacionRegion = value[0].poblacionRegion;

    return map(agrupadosPorFecha, (value, fecha) => {
      const cantidad = sumBy(value, (v) => v.cantidad);
      return {
        region,
        fecha,
        cantidad,
        poblacion: poblacionRegion,
        tasa: cantidad / poblacionRegion,
      };
    });
  }).flat();

  const tasaInternacional = registroNuevosCasosConfirmadosInternacionales.map(
    ({ cantidad, codigo_iso_pais, fecha }) => {
      const pais =
        paises.find((pais) => {
          return pais.codigo_iso === codigo_iso_pais;
        }) ??
        (() => {
          throw Error("País no pudo ser encontrada para " + codigo_iso_pais.toString());
        })();

      return {
        pais: pais.nombre,
        fecha,
        poblacion: pais.poblacion,
        cantidad,
        tasa: cantidad / pais.poblacion,
      };
    }
  );

  return { tasaComunalNacional, tasaRegionalNacional, tasaInternacional };
}

async function getIndicadores() {
  const [tasasContagiados, tasasFallecidos] = await Promise.all([
    TasaContagiadosPoblacion(),
    TasaFallecidosPoblacion(),
  ]);

  const wb = xslx.utils.book_new();

  xslx.utils.book_append_sheet(
    wb,
    xslx.utils.json_to_sheet(tasasFallecidos.tasaNacional),
    "Tasa Fallecidos Nacional"
  );

  xslx.utils.book_append_sheet(
    wb,
    xslx.utils.json_to_sheet(tasasFallecidos.tasaInternacional),
    "Tasa Fallecidos Internacional"
  );

  xslx.utils.book_append_sheet(
    wb,
    xslx.utils.json_to_sheet(tasasContagiados.tasaComunalNacional),
    "Tasa Casos Comunal"
  );
  xslx.utils.book_append_sheet(
    wb,
    xslx.utils.json_to_sheet(tasasContagiados.tasaRegionalNacional),
    "Tasa Casos Regional"
  );
  xslx.utils.book_append_sheet(
    wb,
    xslx.utils.json_to_sheet(tasasContagiados.tasaInternacional),
    "Tasa Casos Internacional"
  );

  const newExcelLocation = path.resolve("./tables/indicadores.xlsx");

  try {
    xslx.writeFile(wb, newExcelLocation);
    console.log("XLSX Creado en " + newExcelLocation);
  } catch (err) {
    console.error(err);
    console.warn("XLSX No pudo ser creado");
  }
}

getIndicadores();
