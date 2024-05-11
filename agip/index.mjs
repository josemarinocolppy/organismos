const AWS = require("aws-sdk");
const mysql = require("mysql");
const readline = require("readline");

const campo1 = 3;
const campo2 = 6;

exports.handler = async (event) => {
  // Configuración de AWS
  AWS.config.update({
    accessKeyId: "AKIA4MTWI44FSBI5SR65",
    secretAccessKey: "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",
    region: "us-east-1",
  });
  // Configuración de MySQL
  const connection = mysql.createConnection({
    host: "lambda.c5220y2ye8uj.us-west-2.rds.amazonaws.com",
    user: "root",
    password: "02082023",
    database: "lambda",
  });

  // Función para leer el archivo desde S3 línea por línea y procesarlo
  const processFileFromS3 = async () => {
    const s3 = new AWS.S3();
    const params = {
      Bucket: "agiii",
      Key: "archivo.txt",
    };

    const response = await s3.getObject(params).promise();
    const fileData = response.Body.toString();

    const lines = fileData.split("\n");

    for (const line of lines) {
      const fields = line.split(";");
      const campos = [fields[campo1], fields[campo2]];
      await insertIntoDatabase(fields);
    }

    // Cierra la conexión a MySQL después de que se completen todas las inserciones
    connection.end();
  };

  // Función para insertar los campos en la base de datos MySQL
  const insertIntoDatabase = (fields) => {
    return new Promise((resolve, reject) => {
      const query = "INSERT INTO agip (cuit, porcentaje) VALUES (?, ?)";
      connection.query(query, fields, (error, results) => {
        if (error) {
          console.error("Error al insertar datos:", error);
          reject(error);
        } else {
          console.log("Datos insertados correctamente");
          resolve();
        }
      });
    });
  };

  try {
    await processFileFromS3();
    return { statusCode: 200, body: "Proceso completado" };
  } catch (error) {
    console.error("Error en el proceso:", error);
    return { statusCode: 500, body: "Error en el proceso" };
  }
};
