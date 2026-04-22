"""
APLICACIÓN WEB - PROCESADOR DE FICHADAS HIKVISION
Optimizado para Render.com
"""

from flask import Flask, request, send_file, jsonify
from flask_cors import CORS
import pandas as pd
from datetime import datetime, timedelta
import os
import tempfile
import traceback
import re

app = Flask(__name__)
CORS(app)

# Configuración
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max

class ProcesadorFichadas:
    """Procesador de fichadas Hikvision - Versión integrada para web"""
    
    def __init__(self):
        self.df_hikvision = None
        self.df_horas_procesadas = None
        self.empleados_procesados = []
        self.errores = []
        self.alertas = []
        self.año = None
        self.mes = None
    
    def cargar_reporte_hikvision(self, ruta_archivo):
        """Carga el archivo AllReport de Hikvision"""
        try:
            df = pd.read_excel(ruta_archivo, sheet_name='Attendance Record')
            
            # Buscar fila de headers
            header_row = None
            for idx, row in df.iterrows():
                if str(row.iloc[0]).strip() == 'Employee ID':
                    header_row = idx
                    break
            
            if header_row is None:
                raise ValueError("No se encontró la fila de headers")
            
            # Extraer metadata
            rango_fechas = df.iloc[2, 0] if len(df) > 2 else "No disponible"
            
            match = re.search(r'(\d{4})/(\d{2})/\d{2}-\d{4}/\d{2}/\d{2}', str(rango_fechas))
            if match:
                self.año = int(match.group(1))
                self.mes = int(match.group(2))
            else:
                hoy = datetime.now()
                self.año = hoy.year
                self.mes = hoy.month
            
            # Crear dataframe limpio
            headers = df.iloc[header_row].values
            df_clean = df.iloc[header_row+1:].copy()
            df_clean.columns = headers
            df_clean = df_clean.dropna(subset=['Employee ID']).reset_index(drop=True)
            
            self.df_hikvision = df_clean
            return True
            
        except Exception as e:
            self.errores.append(f"Error al cargar archivo: {str(e)}")
            return False
    
    def parsear_fichadas_dia(self, texto_fichadas):
        """Parsea las fichadas de un día"""
        if pd.isna(texto_fichadas) or str(texto_fichadas).strip() == '':
            return []
        
        texto = str(texto_fichadas).strip()
        fichadas = re.split(r'\\n|\n', texto)
        fichadas = [f.strip() for f in fichadas if f.strip()]
        
        fichadas_validas = []
        for f in fichadas:
            if re.match(r'^\d{1,2}:\d{2}$', f):
                fichadas_validas.append(f)
        
        return fichadas_validas
    
    def calcular_horas_trabajadas(self, entrada, salida):
        """Calcula horas trabajadas entre entrada y salida"""
        try:
            h_entrada, m_entrada = map(int, entrada.split(':'))
            h_salida, m_salida = map(int, salida.split(':'))
            
            entrada_dt = datetime(2000, 1, 1, h_entrada, m_entrada)
            salida_dt = datetime(2000, 1, 1, h_salida, m_salida)
            
            if salida_dt > entrada_dt:
                diferencia = salida_dt - entrada_dt
                horas_totales = diferencia.total_seconds() / 3600
                return horas_totales, diferencia
            
            elif h_entrada >= 12 and h_salida < 12:
                salida_dt = salida_dt + timedelta(days=1)
                diferencia = salida_dt - entrada_dt
                horas_totales = diferencia.total_seconds() / 3600
                return horas_totales, diferencia
            
            else:
                diferencia = timedelta(0)
                return 0, diferencia
            
        except Exception as e:
            self.alertas.append(f"Error calculando horas: {str(e)}")
            return 0, timedelta(0)
    
    def procesar_empleado(self, fila):
        """Procesa fichadas de un empleado"""
        empleado_id = str(fila['Employee ID']).strip()
        nombre_completo = str(fila['Name']).strip()
        
        partes_nombre = nombre_completo.split()
        if len(partes_nombre) >= 2:
            apellido = partes_nombre[0].upper()
            nombre = ' '.join(partes_nombre[1:]).upper()
        else:
            apellido = nombre_completo.upper()
            nombre = ""
        
        registros_empleado = []
        
        for dia in range(1, 32):
            try:
                col_dia = str(dia)
                if col_dia not in fila.index:
                    continue
                
                fichadas_dia = self.parsear_fichadas_dia(fila[col_dia])
                
                try:
                    fecha = datetime(self.año, self.mes, dia)
                except ValueError:
                    continue
                
                dias_semana = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
                dia_semana = dias_semana[fecha.weekday()]
                
                registro = {
                    'Apellido': apellido,
                    'Nombre': nombre,
                    'Fecha': fecha,
                    'Dia de la semana': dia_semana,
                    'Check in 1': None,
                    'Check out 1': None,
                    'Check in 2': None,
                    'Check out 2': None,
                    'Horas fichadas': timedelta(0),
                    'Comentarios': ''
                }
                
                if len(fichadas_dia) == 0:
                    registro['Check in 1'] = 'F'
                    registro['Horas fichadas'] = timedelta(0)
                
                elif len(fichadas_dia) == 1:
                    h1 = int(fichadas_dia[0].split(':')[0])
                    
                    if h1 < 12 and len(registros_empleado) > 0:
                        registro_anterior = registros_empleado[-1]
                        
                        if registro_anterior['Check in 1'] and registro_anterior['Check in 1'] != 'F' and not registro_anterior['Check out 1']:
                            registro_anterior['Check out 1'] = fichadas_dia[0]
                            horas, duracion = self.calcular_horas_trabajadas(
                                registro_anterior['Check in 1'], fichadas_dia[0]
                            )
                            registro_anterior['Horas fichadas'] = duracion
                            
                            if registro_anterior['Comentarios'] == '⚠️ Solo una fichada':
                                registro_anterior['Comentarios'] = ''
                            
                            registro['Check in 1'] = 'F'
                            registro['Horas fichadas'] = timedelta(0)
                        else:
                            registro['Check in 1'] = fichadas_dia[0]
                            registro['Comentarios'] = '⚠️ Solo una fichada'
                    else:
                        registro['Check in 1'] = fichadas_dia[0]
                        registro['Comentarios'] = '⚠️ Solo una fichada'
                
                elif len(fichadas_dia) == 2:
                    h1 = int(fichadas_dia[0].split(':')[0])
                    h2 = int(fichadas_dia[1].split(':')[0])
                    m1 = int(fichadas_dia[0].split(':')[1])
                    m2 = int(fichadas_dia[1].split(':')[1])
                    
                    tiempo1_minutos = h1 * 60 + m1
                    tiempo2_minutos = h2 * 60 + m2
                    diferencia_minutos = tiempo2_minutos - tiempo1_minutos
                    
                    if h1 < 12 and h2 >= 12 and diferencia_minutos > 720:
                        salida_dia_anterior = fichadas_dia[0]
                        entrada_dia_actual = fichadas_dia[1]
                        
                        registro['Check in 1'] = entrada_dia_actual
                        registro['Check out 1'] = None
                        
                        if len(registros_empleado) > 0:
                            registro_anterior = registros_empleado[-1]
                            if registro_anterior['Check in 1'] and registro_anterior['Check in 1'] != 'F' and not registro_anterior['Check out 1']:
                                registro_anterior['Check out 1'] = salida_dia_anterior
                                horas, duracion = self.calcular_horas_trabajadas(
                                    registro_anterior['Check in 1'], salida_dia_anterior
                                )
                                registro_anterior['Horas fichadas'] = duracion
                                
                                if registro_anterior['Comentarios'] == '⚠️ Solo una fichada':
                                    registro_anterior['Comentarios'] = ''
                    else:
                        registro['Check in 1'] = fichadas_dia[0]
                        registro['Check out 1'] = fichadas_dia[1]
                        horas, duracion = self.calcular_horas_trabajadas(fichadas_dia[0], fichadas_dia[1])
                        registro['Horas fichadas'] = duracion
                
                elif len(fichadas_dia) >= 3:
                    h1 = int(fichadas_dia[0].split(':')[0])
                    h2 = int(fichadas_dia[1].split(':')[0])
                    m1 = int(fichadas_dia[0].split(':')[1])
                    m2 = int(fichadas_dia[1].split(':')[1])
                    
                    tiempo1_minutos = h1 * 60 + m1
                    tiempo2_minutos = h2 * 60 + m2
                    diferencia_minutos = tiempo2_minutos - tiempo1_minutos
                    
                    if h1 < 12 and h2 >= 12 and diferencia_minutos > 720 and len(fichadas_dia) >= 3:
                        salida_dia_anterior = fichadas_dia[0]
                        entrada_dia_actual = fichadas_dia[1]
                        salida_dia_actual = fichadas_dia[2]
                        
                        registro['Check in 1'] = entrada_dia_actual
                        registro['Check out 1'] = salida_dia_actual
                        horas, duracion = self.calcular_horas_trabajadas(entrada_dia_actual, salida_dia_actual)
                        registro['Horas fichadas'] = duracion
                        
                        if len(registros_empleado) > 0:
                            registro_anterior = registros_empleado[-1]
                            if registro_anterior['Check in 1'] and registro_anterior['Check in 1'] != 'F' and not registro_anterior['Check out 1']:
                                registro_anterior['Check out 1'] = salida_dia_anterior
                                horas_ant, duracion_ant = self.calcular_horas_trabajadas(
                                    registro_anterior['Check in 1'], salida_dia_anterior
                                )
                                registro_anterior['Horas fichadas'] = duracion_ant
                                if registro_anterior['Comentarios'] == '⚠️ Solo una fichada':
                                    registro_anterior['Comentarios'] = ''
                    elif len(fichadas_dia) == 4:
                        registro['Check in 1'] = fichadas_dia[0]
                        registro['Check out 1'] = fichadas_dia[1]
                        registro['Check in 2'] = fichadas_dia[2]
                        registro['Check out 2'] = fichadas_dia[3]
                        
                        horas1, duracion1 = self.calcular_horas_trabajadas(fichadas_dia[0], fichadas_dia[1])
                        horas2, duracion2 = self.calcular_horas_trabajadas(fichadas_dia[2], fichadas_dia[3])
                        registro['Horas fichadas'] = duracion1 + duracion2
                    else:
                        registro['Check in 1'] = fichadas_dia[0]
                        registro['Check out 1'] = fichadas_dia[1] if len(fichadas_dia) > 1 else None
                        registro['Comentarios'] = f'⚠️ {len(fichadas_dia)} fichadas'
                
                registros_empleado.append(registro)
                
            except Exception as e:
                continue
        
        return registros_empleado
    
    def procesar_todos_empleados(self):
        """Procesa todos los empleados"""
        todos_registros = []
        
        for idx, fila in self.df_hikvision.iterrows():
            empleado_nombre = str(fila['Name']).strip()
            registros = self.procesar_empleado(fila)
            todos_registros.extend(registros)
            
            if idx < len(self.df_hikvision) - 1:
                fila_vacia = {
                    'Apellido': '', 'Nombre': '', 'Fecha': None,
                    'Dia de la semana': '', 'Check in 1': '', 'Check out 1': '',
                    'Check in 2': '', 'Check out 2': '', 'Horas fichadas': None,
                    'Comentarios': ''
                }
                todos_registros.append(fila_vacia)
            
            self.empleados_procesados.append(empleado_nombre)
        
        self.df_horas_procesadas = pd.DataFrame(todos_registros)
        
        # Validar alertas finales
        df_con_datos = self.df_horas_procesadas[self.df_horas_procesadas['Apellido'] != '']
        for idx, row in df_con_datos.iterrows():
            if row['Comentarios'] == '⚠️ Solo una fichada':
                if row['Check in 1'] and row['Check in 1'] != 'F' and pd.isna(row['Check out 1']):
                    fecha_str = pd.to_datetime(row['Fecha']).strftime('%d/%m') if pd.notna(row['Fecha']) else 'N/A'
                    nombre_completo = f"{row['Nombre']} {row['Apellido']}" if row['Nombre'] else row['Apellido']
                    self.alertas.append(f"{nombre_completo} - {fecha_str}: Solo fichada de entrada")
        
        self.df_horas_procesadas['Fecha'] = pd.to_datetime(
            self.df_horas_procesadas['Fecha'], errors='coerce'
        ).dt.date
        
        def timedelta_to_hhmm(td):
            if pd.isna(td) or td is None or td == 0:
                return None
            if isinstance(td, timedelta):
                total_seconds = int(td.total_seconds())
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                return f"{hours}:{minutes:02d}"
            return td
        
        self.df_horas_procesadas['Horas fichadas'] = self.df_horas_procesadas['Horas fichadas'].apply(timedelta_to_hhmm)
        
        return True
    
    def generar_archivo_salida(self, ruta_salida):
        """Genera el archivo Excel de salida"""
        try:
            from openpyxl.styles import numbers
            
            with pd.ExcelWriter(ruta_salida, engine='openpyxl', datetime_format='DD/MM/YYYY') as writer:
                self.df_horas_procesadas['Fecha'] = pd.to_datetime(
                    self.df_horas_procesadas['Fecha'], errors='coerce'
                )
                
                df_para_excel = self.df_horas_procesadas.copy()
                df_para_excel['Horas fichadas'] = None
                
                df_para_excel.to_excel(writer, sheet_name='Horas', index=False)
                
                worksheet = writer.sheets['Horas']
                
                for row_idx in range(2, len(self.df_horas_procesadas) + 2):
                    cell_fecha = worksheet[f'C{row_idx}']
                    cell_fecha.number_format = 'DD/MM/YYYY'
                    
                    cell_horas = worksheet[f'I{row_idx}']
                    formula = (
                        f'=IF(E{row_idx}="F","",'
                        f'IF(AND(E{row_idx}<>"",F{row_idx}<>""),'
                        f'IF(F{row_idx}<E{row_idx},(F{row_idx}+1)-E{row_idx},F{row_idx}-E{row_idx}),'
                        f'"")'
                        f'+IF(AND(G{row_idx}<>"",H{row_idx}<>""),'
                        f'IF(H{row_idx}<G{row_idx},(H{row_idx}+1)-G{row_idx},H{row_idx}-G{row_idx}),'
                        f'0))'
                    )
                    cell_horas.value = formula
                    cell_horas.number_format = '[h]:mm'
            
            return True
            
        except Exception as e:
            self.errores.append(f"Error generando archivo: {str(e)}")
            traceback.print_exc()
            return False

# Rutas de la aplicación

@app.route('/')
def index():
    """Página principal"""
    html_path = os.path.join(os.path.dirname(__file__), 'index.html')
    if os.path.exists(html_path):
        with open(html_path, 'r', encoding='utf-8') as f:
            return f.read()
    return """
    <html><body style="font-family: sans-serif; text-align: center; padding: 50px;">
    <h1>⚠️ Error</h1>
    <p>Falta el archivo index.html</p>
    </body></html>
    """, 404

@app.route('/api/procesar', methods=['POST'])
def procesar_fichadas():
    """Procesa el archivo AllReport"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No se envió archivo'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'Archivo vacío'}), 400
        
        # Guardar archivo temporalmente
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xls') as temp_input:
            file.save(temp_input.name)
            input_path = temp_input.name
        
        # Procesar
        procesador = ProcesadorFichadas()
        
        if not procesador.cargar_reporte_hikvision(input_path):
            os.unlink(input_path)
            return jsonify({'error': 'Error cargando archivo', 'detalles': procesador.errores}), 400
        
        if not procesador.procesar_todos_empleados():
            os.unlink(input_path)
            return jsonify({'error': 'Error procesando', 'detalles': procesador.errores}), 400
        
        # Generar salida
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_output:
            output_path = temp_output.name
        
        if not procesador.generar_archivo_salida(output_path):
            os.unlink(input_path)
            return jsonify({'error': 'Error generando archivo', 'detalles': procesador.errores}), 400
        
        os.unlink(input_path)
        
        # Enviar archivo directamente
        return send_file(
            output_path,
            as_attachment=True,
            download_name=f'Nomina_Procesada_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': 'Error interno', 'detalles': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
