/*Integrantes:
	Fabián Paillacán
	Diego Ruiz*/
#include <algorithm>
#include <chrono>  
#include <fstream>
#include <iomanip>  
#include <iostream>
#include <limits>
#include <map>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <omp.h>

using namespace std;
using namespace std::chrono;  // Para facilitar el uso de las herramientas de
                              // <chrono>

const char DELIMITADOR = ';';
const char DELIMITADOR_2 = '\t';  // Cambiamos el delimitador a tabulador

struct Canasta {
  string fechaCreacion;
  int numeroBoleta;
  int numeroTienda;
  string nombreFantasia;
  string categoriaTienda;
  string tipoEnvio;
  string SKU;
  int cantidad;
  string nombreProducto;
  double monto;
};

struct PreciosHistoricos {
  string fechaPrecio;
  double price;  // Ahora el precio se guarda como double
};

string QuitarComillas(const string& str) {
  if (str.length() >= 2 && str[0] == '"' && str[str.length() - 1] == '"') {
    return str.substr(1, str.length() - 2);
  }
  return str;
}

int stringToInt(const string& str) {
  istringstream iss(str);
  int value;
  iss >> value;
  if (iss.fail()) {
    throw invalid_argument("No se puede convertir a entero");
  }
  return value;
}

double stringToDouble(const string& str) {
  istringstream iss(str);
  double value;
  iss >> value;
  if (iss.fail()) {
    throw invalid_argument("No se puede convertir a Double");
  }
  return value;
}

string TruncarFecha(const string& dateTime) { return dateTime.substr(0, 10); }


bool validarFecha(const string& fecha) {
  if (fecha.size() != 10) return false;
  if (fecha[4] != '-' || fecha[7] != '-') return false; 
  for (size_t i = 0; i < fecha.size(); ++i) {
    if (i == 4 || i == 7) continue;
    if (!isdigit(fecha[i])) return false;
  }
  return true;
}

void LeerArchivo(vector<Canasta>& canastas) {
  string filename = "pd.csv";
  ifstream file(filename.c_str());
  if (!file.is_open()) {
    cerr << "Error al abrir el archivo " << filename << endl;
    return;
  }
  cout << "Leyendo csv... " << endl;

  string line;
  int fechaNoPermitida = 0;
  while (getline(file, line)) {
    stringstream ss(line);
    string value;
    Canasta canasta;

    try {
      getline(ss, value, DELIMITADOR);
      canasta.fechaCreacion = TruncarFecha(QuitarComillas(value));

      getline(ss, value, DELIMITADOR);
      canasta.numeroBoleta = stringToInt(QuitarComillas(value));

      getline(ss, value, DELIMITADOR);
      canasta.numeroTienda = stringToInt(QuitarComillas(value));

      getline(ss, value, DELIMITADOR);
      canasta.nombreFantasia = QuitarComillas(value);

      getline(ss, value, DELIMITADOR);
      canasta.categoriaTienda = QuitarComillas(value);

      getline(ss, value, DELIMITADOR);
      canasta.tipoEnvio = QuitarComillas(value);

      getline(ss, value, DELIMITADOR);
      canasta.SKU = QuitarComillas(value);

      getline(ss, value, DELIMITADOR);
      canasta.cantidad = stringToInt(QuitarComillas(value));

      getline(ss, value, DELIMITADOR);
      canasta.nombreProducto = QuitarComillas(value);

      getline(ss, value, DELIMITADOR);
      canasta.monto = stringToDouble(QuitarComillas(value));
    } catch (const invalid_argument& e) {
      continue;
    } catch (const out_of_range& e) {
      continue;
    }
    if (validarFecha(canasta.fechaCreacion)) {
      canastas.push_back(canasta);
    } else {
      fechaNoPermitida++;
    }
  }
  cout << "Fechas no permitidas: " << fechaNoPermitida << endl;
  file.close();
}


map<string, map<string, pair<double, int>>> agruparPorMesYProducto(
    const vector<Canasta>& canastas, const string& fecha) {
  map<string, map<string, pair<double, int>>> productosPorMes;
  #pragma omp parallel for num_threads(12)
  for (int i = 0; i < canastas.size(); ++i) {
  	
    if (canastas[i].fechaCreacion.size() >= 7 &&
        canastas[i].fechaCreacion.substr(0, 4) == fecha) {
      string mes = canastas[i].fechaCreacion.substr(0, 7);  // Formato "YYYY-MM"
      productosPorMes[mes][canastas[i].SKU].first += canastas[i].monto;
      productosPorMes[mes][canastas[i].SKU].second += 1;
    }
  }
  return productosPorMes;
}

set<string> obtenerProductosEnTodosLosMeses(
    const map<string, map<string, pair<double, int>>>& productosPorMes) {
  map<string, int> contadorProductos;
  int numMeses = productosPorMes.size();

  for (const auto& entry : productosPorMes) {
    for (const auto& producto : entry.second) {
      contadorProductos[producto.first]++;
    }
  }

  set<string> productosEnTodosLosMeses;
  for (const auto& entry : contadorProductos) {
    if (entry.second == numMeses) {
      productosEnTodosLosMeses.insert(entry.first);
    }
  }
  return productosEnTodosLosMeses;
}

void leerPreciosHistoricos(vector<PreciosHistoricos>& precios) {
  ifstream file("Datos históricos PEN_CLP.xlsx - Datos históricos PEN_CLP.tsv");
  if (!file.is_open()) {
    cerr << "No se pudo abrir el archivo Datos históricos PEN_CLP.xlsx - Datos "
            "históricos PEN_CLP.tsv. Verifica la ruta y los permisos."
         << endl;
    return;
  }

  string line;
  int lineNumber = 0;
  while (getline(file, line)) {
    lineNumber++;

    if (lineNumber < 7) {
      continue;  // Saltar las primeras 6 líneas (contando desde 1)
    }

    stringstream ss(line);
    string item;
    PreciosHistoricos ph;
    int column = 0;

    while (getline(ss, item, DELIMITADOR_2)) {
      // Eliminar comillas al inicio y final
      if (!item.empty() && item.front() == '"' && item.back() == '"') {
        item = item.substr(1, item.size() - 2);
      }

      if (column == 0) {
        ph.fechaPrecio = item;
      } else if (column == 1) {
        // Eliminar el símbolo '$' del precio y luego convertirlo a double
        item.erase(remove(item.begin(), item.end(), '$'), item.end());
        // Reemplazar ',' por '.' si es necesario
        replace(item.begin(), item.end(), ',', '.');
        try {
          ph.price = stod(item);  // Convertir string a double
        } catch (const std::exception& e) {
          cerr << "Error al convertir precio a double: " << e.what() << endl;
          ph.price = 0.0;  // Asignar un valor por defecto en caso de error
        }
      }

      column++;
      if (column > 1) break;  // Solo nos interesa la primera y segunda columna
    }

    // Verificar el año de la fechaPrecio (suponiendo que está en formato
    // "d/m/yyyy")
    if (!ph.fechaPrecio.empty()) {
      size_t pos = ph.fechaPrecio.find('/');
      if (pos != string::npos) {
        int day = stoi(ph.fechaPrecio.substr(0, pos));
        size_t pos2 = ph.fechaPrecio.find('/', pos + 1);
        if (pos2 != string::npos) {
          int month = stoi(ph.fechaPrecio.substr(pos + 1, pos2 - pos - 1));
          int year = stoi(ph.fechaPrecio.substr(pos2 + 1));
          // Validar rango de fechas
          if (year >= 2021 && year <= 2024 && month >= 1 && month <= 12 &&
              day >= 1 && day <= 31) {
            precios.push_back(ph);
          }
        }
      }
    }
  }

  file.close();
}

void calcularPromedioPreciosPorMes(vector<PreciosHistoricos>& precios) {
  map<pair<int, int>, pair<double, int>>
      promedios;  // (año, mes) -> (suma de precios, cantidad de elementos)
  // Calcular promedios por mes y año
  #pragma omp parallel for num_threads(12)
  for (int i=0; i < precios.size();++i) {
    // Obtener año y mes de la fechaPrecio
    size_t pos = precios[i].fechaPrecio.find('/');
    if (pos != string::npos) {
      int day = stoi(precios[i].fechaPrecio.substr(0, pos));
      size_t pos2 = precios[i].fechaPrecio.find('/', pos + 1);
      if (pos2 != string::npos) {
        int month = stoi(precios[i].fechaPrecio.substr(pos + 1, pos2 - pos - 1));
        int year = stoi(precios[i].fechaPrecio.substr(pos2 + 1));
        // Validar rango de fechas nuevamente (aunque debería estar validado ya
        // en leerPreciosHistoricos)
        if (year >= 2021 && year <= 2024 && month >= 1 && month <= 12 &&
            day >= 1 && day <= 31) {
          promedios[{year, month}].first += precios[i].price;
          #pragma omp atomic
		  promedios[{year, month}].second++;
        }
      }
    }
  }

  // Limpiar el vector precios
  precios.clear();

  // Actualizar el vector con los promedios calculados
  for (const auto& entry : promedios) {
    auto key = entry.first;
    auto value = entry.second;
    double promedio = value.first / value.second;
    precios.push_back(
        {to_string(key.second) + "/" + to_string(key.first), promedio});
  }
}

//-----------------------------------------------------------------------------------------------------------------------------------------
// Implementación de la función calcularIPCAnual como se mostró anteriormente
void calcularInflacion(const set<string>& fechas,
                       const vector<Canasta>& canastas,
                       const vector<PreciosHistoricos>& precios) {
   #pragma omp parallel for  num_threads(12)                	
  for (int i = 0; i < fechas.size(); ++i) {
  	auto fecha = fechas.begin();
  	advance(fecha,i);
    auto productosPorMes = agruparPorMesYProducto(canastas, *fecha);
    auto productosEnTodosLosMeses =
        obtenerProductosEnTodosLosMeses(productosPorMes);

    std::map<std::string, double> sumaPromediosPorMes;
    std::map<std::string, int> totalOcurrenciasPorMes;

    // Calcular sumas y promedios por mes y producto
    
    for (int z = 0; z < productosEnTodosLosMeses.size(); ++z) {
    	auto producto = productosEnTodosLosMeses.begin();
  	advance(producto ,z);
      for (const auto& mes : productosPorMes) {
        if (mes.second.find(*producto) != mes.second.end()) {
          double montoDelMes = mes.second.at(*producto).first;
          int cantidadEnMes = mes.second.at(*producto).second;
          double promedioPorMes = montoDelMes / cantidadEnMes;
		#pragma omp atomic
          sumaPromediosPorMes[mes.first] += promedioPorMes;
          #pragma omp atomic
          totalOcurrenciasPorMes[mes.first] += cantidadEnMes;
        }
      }
    }

    double totales = 0.0;
    double IPC = 0.0;
    double PrimeraVez = 0.0;
    double IPC_anterior = 0.0;
    double Inflacion = 0.0;
    int contador_IPC = 0;
    int Conta_historia = 0;
    double mes_chileno = 0.0;
    double PrimeraVez_chile = 0.0;
    double IPC_ch = 0.0;
    double Inflacion_chile = 0.0;
    double IPC_anterior_ch = 0.0;

    // Calcular IPC y mostrar resultados por mes
    #pragma omp parallel for  num_threads(12)
	for (int y = 0; y < sumaPromediosPorMes.size(); ++y) {
    		auto mes = sumaPromediosPorMes.begin();
  	advance(mes,y);
      mes_chileno = mes->second * precios[Conta_historia].price;
      if (mes->first.substr(0, 7) == "2021-02" ||
          mes->first.substr(0, 7) == "2022-01" ||
          mes->first.substr(0, 7) == "2023-01" ||
          mes->first.substr(0, 7) == "2024-01") {
        PrimeraVez = mes->second;
        PrimeraVez_chile = mes_chileno;
      }
      IPC_ch = (mes_chileno / PrimeraVez_chile) * 100;
      IPC = (mes->second / PrimeraVez) * 100;
      // std::cout << "El IPC del mes: " << mes.first << " es: " << std::fixed
      // << std::setprecision(10) << IPC << std::endl;

      totales += mes->second;

      if (contador_IPC > 0) {
        Inflacion = ((IPC - IPC_anterior) / IPC_anterior) * 100;
        Inflacion_chile = ((IPC_ch - IPC_anterior_ch) / IPC_anterior_ch) * 100;
        std::cout << "Calculo de la inflacion Peruana: " << Inflacion
                  << " fecha: " << mes->first << std::endl;
        std::cout << "Calculo de la inflacion Chilena: " << Inflacion_chile
                  << " fecha: " << mes->first << std::endl;
      }

      contador_IPC++;
      IPC_anterior = IPC;
      IPC_anterior_ch = IPC_ch;
      Conta_historia++;
    }
  }
}
void participantes (){
cout<<"Participantes del proyecto: "<<endl;
cout<< "Fabian Paillacan"<<endl;
cout<<"Diego Ruiz"<< endl;
}
//------------------------------------------------------------------------------------------------------------------------------------------------
int main() {
  // Inicio de la medición de tiempo
  auto start = high_resolution_clock::now();
  set<string> fechas = {"2021", "2022", "2023", "2024"};
  vector<Canasta> canastas;
 
  // Leer el archivo y llenar el vector de canastas
  LeerArchivo(canastas);

  // Leer precios históricos
  vector<PreciosHistoricos> precios;
  leerPreciosHistoricos(precios);

  // Calcular promedio de precios por mes y año
  calcularPromedioPreciosPorMes(precios);

  calcularInflacion(fechas, canastas, precios);
  
  participantes();
  

  // Fin de la medición de tiempo
  auto end = high_resolution_clock::now();
  auto duration = duration_cast<milliseconds>(end - start);

  cout << "Tiempo de ejecucion paralelo: " << duration.count() << " ms" << endl;
  return 0;
}