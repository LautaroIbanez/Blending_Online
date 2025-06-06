import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import tempfile
import os
import webbrowser
import pandas as pd
from datetime import datetime
import plotly.graph_objects as go
import numpy as np
from plotly.subplots import make_subplots
from dictionary import header_translation, tag_header_translation,alr_eve_header_translation
import webbrowser
import time
import mysql.connector
import threading
from openai_analyzer import OpenAIAnalyzer
import logging
import json

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ai_analysis.log'),
        logging.StreamHandler()
    ]
)

# Global variables to store selected items for each listbox separately
selected_dates = []
selected_hourly_productions = []
selected_event_types = []
selected_components = []
bb_eve_data = pd.DataFrame()
bb_op_data = pd.DataFrame()
bb_alr_data = pd.DataFrame()

# Define all_plots_df globally
all_plots_df = pd.DataFrame()
plot_counter = 0
prev_fig_unica = None
prev_all_plots_df = pd.DataFrame()
prev_event_times = set()

# Inicializar el analizador de OpenAI
try:
    analyzer = OpenAIAnalyzer(model="gpt-3.5-turbo")
except ValueError as e:
    messagebox.showwarning(
        "Configuración de OpenAI",
        f"Error al inicializar OpenAI: {str(e)}\n\n"
        "Para configurar la API key de OpenAI, puedes:\n"
        "1. Configurar la variable de entorno OPENAI_API_KEY:\n"
        "   - En Windows (PowerShell): $env:OPENAI_API_KEY='tu-api-key'\n"
        "   - En Windows (CMD): set OPENAI_API_KEY=tu-api-key\n"
        "2. O proporcionar la API key al inicializar el analizador:\n"
        "   analyzer = OpenAIAnalyzer(api_key='tu-api-key', model='gpt-3.5-turbo')"
    )
    analyzer = None
except Exception as e:
    messagebox.showwarning("Error", f"Error inesperado al inicializar OpenAI: {str(e)}")
    analyzer = None

def update_listbox_state(listbox, state):
    listbox.config(state=state)

def reset_global_variables():
    global selected_dates, selected_hourly_productions, selected_event_types, selected_components
    global bb_eve_data, bb_op_data, bb_alr_data, data

    selected_dates = []
    selected_hourly_productions = []
    selected_event_types = []
    selected_components = []
    data = pd.DataFrame()
    bb_eve_data = pd.DataFrame()
    bb_op_data = pd.DataFrame()
    bb_alr_data = pd.DataFrame()
    
    # Clear only dynamic content in Telemetry Frame
    for widget in telemetry_frame.winfo_children():
        if widget not in {last_version_label, last_version_value, telemetry_data_label, instruction_label}:
            widget.destroy()

def load_data():
    global data, bb_op_data, bb_eve_data, bb_alr_data

    # Reset global variables
    reset_global_variables()

    csv_paths = filedialog.askopenfilenames(filetypes=[("CSV files", "*.csv")])
    
    # Create a label for the loading message
    loading_label = ttk.Label(csv_frame, text="Loading...")
    loading_label.grid(row=1, column=0, columnspan=2, padx=20, pady=10)

    root.update()  # Update the root window to show the progress bar

    success = True  # Flag to track the success of file reading operations
    
    # Initialize variables to store the paths of files to be used for 'data', 'bb_op_data', and 'bb_eve_data'
    data_csv_path = None
    bb_op_csv_path = None
    bb_eve_csv_path = None
    bb_alr_csv_path = None

    if csv_paths:
        # Lista de prefijos válidos
        valid_prefixes = ["BBProdMon_", "BBOp_", "BBEve_", "BBEvent_", "BBAlr_", "BBAlarm_", "P_", "E_"]

        # Comprobar si algún archivo no comienza con un prefijo válido
        invalid_files = [os.path.basename(path) for path in csv_paths if not any(os.path.basename(path).startswith(prefix) for prefix in valid_prefixes)]

        if invalid_files:
            messagebox.showerror("Error", f"The following files must start with one of the valid prefixes: {', '.join(valid_prefixes)}\nInvalid files:\n{', '.join(invalid_files)}")
            return  # Salir si hay archivos no válidos

        # Actualizar el label con los nombres de los archivos seleccionados
        files_selected_label.config(text="\n".join(os.path.basename(path) for path in csv_paths))
    else:
        files_selected_label.config(text="No files selected")

    
    bbop_selected = False
    bbeve_selected = False
    bbalr_selected = False

    for csv_path in csv_paths:
        try:
            if os.path.basename(csv_path).startswith("BBProdMon_") or os.path.basename(csv_path).startswith("P_"):
                data_csv_path = csv_path
                
                # If the file is named as "P_", update the column names
                if os.path.basename(csv_path).startswith("P_"):
                    # Read the CSV file into the data DataFrame
                    data = pd.read_csv(data_csv_path)
                    
                    # Define the new column name and fill it with zeros
                    data['Sensor'] = 0
                    
                    # Rename columns
                    data.rename(columns={
                        "Record n.": "Record no.",
                        "Date": "Date [dd/mm/yyyy]",
                        "Time": "Time [hh:mm:ss]",
                        "Event type": "Event type",
                        "Ev DV": "Ev DV",
                        "Ev MV": "Ev MV",
                        "SV": "SV",
                        "PV": "PV",
                        "Pid P-Term.": "Pid P-Term.",
                        "Pid I-Term.": "Pid I-Term.",
                        "Pid D-Term.": "Pid D-Term."
                    }, inplace=True)
                    
                    # Check if dates contain '.' instead of '/'
                    if data['Date [dd/mm/yyyy]'].str.contains('.').any():
                        # Replace '.' with '/'
                        data['Date [dd/mm/yyyy]'] = data['Date [dd/mm/yyyy]'].str.replace('.', '/')
                        # Convert the year from 'yy' to 'yyyy' format
                        data['Date [dd/mm/yyyy]'] = data['Date [dd/mm/yyyy]'].apply(lambda x: '/'.join(['{:02d}'.format(int(part)) if i != 2 else '20{:02d}'.format(int(part)) for i, part in enumerate(x.split('/'))]))
                                            
            elif "BBOp_" in os.path.basename(csv_path) or os.path.basename(csv_path).startswith("E_"):
                bb_op_csv_path = csv_path
                bbop_selected = True
            elif "BBE" in os.path.basename(csv_path):
                bb_eve_csv_path = csv_path
                bbeve_selected = True
            elif "BBA" in os.path.basename(csv_path):
                bb_alr_csv_path = csv_path
                bbalr_selected = True
            else:
                continue
        except Exception as e:
            success = False
    
    if (bbeve_selected and not bbop_selected) or (bbalr_selected and not bbop_selected):
        messagebox.showinfo("Warning", "Events or Alarms would not be plotted without Black-Box Operation File.")
    
    # Translate files header if necessary:
    if bbop_selected:
        bb_op_data = load_BBOp_data(bb_op_csv_path)  # Load BBOp_ data if selected
        
    
    if data_csv_path is None:
        #messagebox.showerror("Error", "No file with the specified naming convention was selected.")
        success = False

    if success:  # Proceed only if all file paths were successfully determined
        try:
            # Procesar solo si es un archivo BBProdMon_
            if data_csv_path and os.path.basename(data_csv_path).startswith("BBProdMon_"):
                data = pd.read_csv(data_csv_path)
                print("ok 1")
                # Asegúrate de que las columnas necesarias existen antes de proceder
                required_columns = ["Date [dd/mm/yyyy]", "Time [hh:mm:ss]"]
                if all(col in data.columns for col in required_columns):
                    # Crear columna Datetime combinando fecha y hora
                    data['Datetime'] = pd.to_datetime(
                        data['Date [dd/mm/yyyy]'] + ' ' + data['Time [hh:mm:ss]'], 
                        dayfirst=True
                    )
                    print("ok 2")
                else:
                    raise ValueError("Missing required columns for Datetime creation in BBProdMon_ file.")
                
                # Llamar a la función add_microseconds
                data = add_microseconds(data)
                print("ok 3")

                # Actualizar datos disponibles y los estados de las listboxes
                update_available_data()
                print("ok 4")
                update_listbox_state(listbox_dates, tk.NORMAL)
                print("ok 5")
                update_listbox_state(listbox_hourly_productions, tk.DISABLED)
                print("ok 6")
                update_listbox_state(listbox_event_types, tk.DISABLED)
                print("ok 7")
            else:
                print("File is not a BBProdMon_ file, skipping Datetime processing.")
        except Exception as e:
            messagebox.showerror("Error", f"1 An error occurred while uploading the data CSV file: {str(e)}")
            success = False

    
        if bb_op_csv_path is not None:
            try:
                bb_op_data = pd.read_csv(bb_op_csv_path)
                bb_op_data = apply_header_translations(bb_op_data)
                print(f"bb_op_data : {bb_op_data.columns}")
                
                # Ensure the column 'Fuso orario' exists
                if 'Fuso orario' not in bb_op_data.columns:
                    raise ValueError("Column 'Fuso orario' not found in the dataset.")
                
                # Handle optional columns 'Latitudine' and 'Longitudine'
                latitude_data = "N/A"
                longitude_data = "N/A"
                if 'Latitudine' in bb_op_data.columns and 'Longitudine' in bb_op_data.columns:
                    latitude_data = bb_op_data['Latitudine'].iloc[-1]
                    longitude_data = bb_op_data['Longitudine'].iloc[-1]
                print(f"Latitude: {latitude_data}, Longitude: {longitude_data}")
                
                # Convert 'Ora start' to datetime
                bb_op_data['Ora start'] = pd.to_datetime(bb_op_data['Ora start'], format='%d/%m/%Y %H:%M:%S')
                
                # Initialize the hour_offsets list
                hour_offsets = []
                
                for offset_str in bb_op_data['Fuso orario']:
                    sign_index = offset_str.find('+')
                    if sign_index == -1:
                        sign_index = offset_str.find('-')
                    #print(f"sign_index= {sign_index}")
                    
                    if sign_index != -1:
                        hour_offset_str = offset_str[sign_index:]
                        hour_offsets.append(hour_offset_str)
                    else:
                        hour_offsets.append(None)
                
                #print(f"hour offset: {hour_offsets}")
                bb_op_data['Hour Offset'] = hour_offsets

            except Exception as e:
                print("Error encountered:", str(e))
                success = False
    
        if bb_alr_csv_path is not None:
            try:
                bb_alr_data = pd.read_csv(bb_alr_csv_path)
                bb_alr_data.rename(columns=alr_eve_header_translation, inplace=True)
                print(f"bb_alr : {bb_alr_data.head(5)}")
                # Other processing for 'bb_eve_data' or ...
                bb_alr_data['Ora'] = pd.to_datetime(bb_alr_data['Ora'], format='%d/%m/%Y %H:%M:%S')                
                for index, row in bb_alr_data.iterrows():
                    eve_date = row['Ora'].date()
                    matching_op_row = bb_op_data[bb_op_data['Ora start'].dt.date == eve_date]
                    if not matching_op_row.empty:
                        offset = matching_op_row.iloc[0]['Hour Offset']
                        print(f"offset: {offset}")
                        if offset is not None:
                            if ':' in offset:
                                offset_parts = offset.split(':')
                                if len(offset_parts) == 2:  # Adjusted to check for two parts only
                                    try:
                                        offset_timedelta = pd.to_timedelta(':'.join(offset_parts) + ':00')
                                        bb_alr_data.at[index, 'Ora'] -= offset_timedelta
                                    except ValueError as ve:
                                        continue
                                else:
                                    continue
                            else:
                                continue
                        else:
                            continue
                    else:
                        continue
            except Exception as e:
                success = False
    
        if bb_eve_csv_path is not None:
            try:
                bb_eve_data = pd.read_csv(bb_eve_csv_path)
                bb_eve_data.rename(columns=alr_eve_header_translation, inplace=True)
                print(f"bb_eve : {bb_eve_data.head(5)}")
                # Other processing for 'bb_eve_data'...
                bb_eve_data['Ora'] = pd.to_datetime(bb_eve_data['Ora'], format='%d/%m/%Y %H:%M:%S')
                for index, row in bb_eve_data.iterrows():
                    eve_date = row['Ora'].date()
                    matching_op_row = bb_op_data[bb_op_data['Ora start'].dt.date == eve_date]
                    if not matching_op_row.empty:
                        offset = matching_op_row.iloc[0]['Hour Offset']
                        if offset is not None:
                            if ':' in offset:
                                offset_parts = offset.split(':')
                                if len(offset_parts) == 2:  # Adjusted to check for two parts only
                                    try:
                                        offset_timedelta = pd.to_timedelta(':'.join(offset_parts) + ':00')
                                        bb_eve_data.at[index, 'Ora'] -= offset_timedelta
                                    except ValueError as ve:
                                        continue
                                else:
                                    continue
                            else:
                                continue
                        else:
                            continue
                    else:
                        continue

            except Exception as e:
                success = False
    
    # Stop the spinning wheel and close the loading window when done
    loading_label.destroy()
    
    if success:
        messagebox.showinfo("Success", "Data loaded successfully!")


# Function to update the time listbox based on the selected date
def update_time_listbox(event):
    selected_date = pd.to_datetime(date_combobox.get(), dayfirst=True).date()  # Convert selected date to yyyy-mm-dd format
    filtered_data = bb_op_data[pd.to_datetime(bb_op_data["Ora start"], dayfirst=True).dt.date == selected_date]
    ora_listbox.delete(0, tk.END)  # Clear previous values
    ora_values = filtered_data.apply(lambda row: f"{pd.to_datetime(row['Ora start'], dayfirst=True).strftime('%H:%M:%S')} - {pd.to_datetime(row['Ora stop'], dayfirst=True).strftime('%H:%M:%S')}", axis=1)  # Extract and concatenate hour parts
    for value in ora_values:
        ora_listbox.insert(tk.END, value)

def load_BBOp_data(bb_op_csv_path):
    global bb_op_data
    try:
        # Load BBOp_ data
        bb_op_data = pd.read_csv(bb_op_csv_path)
        bb_op_data = apply_header_translations(bb_op_data)  # Translate headers if needed
        print(f"bb_op_data : {bb_op_data.columns}")
        update_last_version(bb_op_data)
        # Extract dates for the date combobox
        dates = pd.to_datetime(bb_op_data["Ora start"], dayfirst=True).dt.date.unique()
        dates = [date.strftime('%d/%m/%Y') for date in dates]
        dates.insert(0, "")  # Add an empty option at the beginning
        date_combobox["values"] = dates
        
        # Clear and reset the time listbox
        ora_listbox.delete(0, tk.END)

    except Exception as e:
        messagebox.showerror("Error", f"2 An error occurred while loading BBOp_ data: {str(e)}")
        return None

    return bb_op_data

def update_last_version(bb_op_data):
    try:
        

        # Verificar si existe la columna 'Software'
        if "Software" not in bb_op_data.columns:
            raise ValueError("The column 'Software' is not present in the file after cleaning.")

        # Obtener el último valor de la columna 'Software'
        last_version = bb_op_data["Software"].iloc[-1]
        last_version_var.set(last_version)  # Actualizar la variable vinculada a la GUI
        print(f"Last Software Version: {last_version}")

    except Exception as e:
        # Manejar errores y mostrar mensaje en la GUI
        last_version_var.set("Error")
        print(f"Error reading last version from file: {e}")


# Function to apply header translations
def apply_header_translations(df):
    # Crear diccionarios de traducción directamente a partir de los nombres originales
    header_translation_normalized = {k: v for k, v in header_translation.items()}
    tag_translation_normalized = {k: v for k, v in tag_header_translation.items()}

    # Iterar por las columnas y aplicar la traducción si es necesaria
    for col in df.columns:
        original_col = col  # Mantener el nombre original exactamente como está
        translated_col = original_col  # Mantener el nombre original por defecto

        # Verificar si la columna coincide con algún tag en el diccionario de traducción por tags
        for tag, translation in tag_translation_normalized.items():
            if tag in original_col:  # Si el tag está contenido en el nombre de la columna
                translated_col = translation  # Aplicar la traducción completa
                break  # Salir del bucle una vez se encuentra una coincidencia

        # Aplicar traducción simple si no se encontró una coincidencia por tags
        if translated_col == original_col:  # Solo si no fue reemplazado por un tag
            translated_col = header_translation_normalized.get(original_col, original_col)

        # Renombrar la columna en el DataFrame
        df.rename(columns={original_col: translated_col}, inplace=True)

    return df


def update_selected_data(event):
    global bb_op_data
    inerte_a = inerte_a2 = inerte_b = inerte_c = inerte_d = inerte_e = dosaggio = 0
    inerte_list = ["A", "A2", "B", "C", "D", "E"]
    try:
        selected_index = ora_listbox.curselection()
        if selected_index:
            selected_index = selected_index[-1]  # Only consider the first selected index
            selected_date = pd.to_datetime(date_combobox.get(), dayfirst=True).date()  # Convert selected date to yyyy-mm-dd format
            filtered_data = bb_op_data[pd.to_datetime(bb_op_data["Ora start"], dayfirst=True).dt.date == selected_date]
            selected_row = filtered_data.iloc[selected_index]

            # Reset all inert values and dosaggio for each row
            inerte_a = inerte_a2 = inerte_b = inerte_c = inerte_d = inerte_e = dosaggio = 0

            for inerte in inerte_list:
                selected_row = filtered_data.iloc[selected_index]  # Access the data of the currently selected row
                if selected_row.get(f"Inerte {inerte[-1]} [kg]", 0):
                    inerte_kg = selected_row.get(f"Inerte {inerte[-1]} [kg]", 0)
                    acqua_eff = selected_row.get(f"Acqua efficace Inerte {inerte[-1]} [l]", 0)
                    umedita_eff = acqua_eff / inerte_kg
                    if inerte == "A":
                        inerte_a = inerte_kg * (1 - umedita_eff)
                        if inerte_a != 0:
                            dosaggio += selected_row.get("Dosaggio Inerte A [kg/m³]", 0)
                    elif inerte == "A2":
                        inerte_a2 = inerte_kg * (1 - umedita_eff)
                        if inerte_a2 != 0:
                            dosaggio += selected_row.get("Dosaggio Inerte A2 [kg/m³]", 0)
                    elif inerte == "B":
                        inerte_b = inerte_kg * (1 - umedita_eff)
                        if inerte_b != 0:
                            dosaggio += selected_row.get("Dosaggio Inerte B [kg/m³]", 0)
                    elif inerte == "C":
                        inerte_c = inerte_kg * (1 - umedita_eff)
                        if inerte_c != 0:
                            dosaggio += selected_row.get("Dosaggio Inerte C [kg/m³]", 0)
                    elif inerte == "D":
                        inerte_d = inerte_kg * (1 - umedita_eff)
                        if inerte_d != 0:
                            dosaggio += selected_row.get("Dosaggio Inerte D [kg/m³]", 0)
                    elif inerte == "E":
                        inerte_e = inerte_kg * (1 - umedita_eff)
                        if inerte_e != 0:
                            dosaggio += selected_row.get("Dosaggio Inerte E [kg/m³]", 0)
            # Considering all Aggregates even if some of them are zero
            m3 = inerte_a + inerte_a2 + inerte_b + inerte_c + inerte_d + inerte_e

            # Debugging: Print calculated values for m3 and dosaggio
            print(f"m3: {m3}, dosaggio: {dosaggio}")
            if dosaggio != 0:
                m3 = m3 / dosaggio
            else:
                if selected_row.get("Dosaggio Cemento [kg/m³]", 0) != 0:
                    m3 = m3 / selected_row.get("Dosaggio Cemento [kg/m³]", 0)
                elif selected_row.get("Dosaggio Acqua [l/m³]", 0) != 0:
                    m3 = m3 / selected_row.get("Dosaggio Acqua [l/m³]", 0)
                elif selected_row.get("Dosaggio Additivo 1 [l/m³]", 0) != 0:
                    m3 = m3 / selected_row.get("Dosaggio Additivo 1 [l/m³]", 0)
                elif selected_row.get("Dosaggio Additivo 2 [l/m³]", 0) != 0:
                    m3 = m3 / selected_row.get("Dosaggio Additivo 2 [l/m³]", 0)
                elif selected_row.get("Dosaggio Additivo 3 [l/m³]", 0) != 0:
                    m3 = m3 / selected_row.get("Dosaggio Additivo 3 [l/m³]", 0)
            selected_data_textbox.delete("1.0", tk.END)  # Clear previous content
        # Print initial fields
            selected_data_textbox.insert(tk.END, "Record No: {}\n".format(selected_row.get("Record no.", "")))
            selected_data_textbox.insert(tk.END, "Software: {}\n".format(selected_row.get("Software", "")))
            selected_data_textbox.insert(tk.END, "S/N macchina: {}\n".format(selected_row.get("S/N macchina", "")))
            selected_data_textbox.insert(tk.END, "Nome azienda: {}\n".format(selected_row.get("Nome azienda", "")))
            selected_data_textbox.insert(tk.END, "Cliente: {}\n".format(selected_row.get("Cliente", "")))
            selected_data_textbox.insert(tk.END, "Ora start: {}\n".format(selected_row.get("Ora start", "")))
            selected_data_textbox.insert(tk.END, "Ora stop: {}\n".format(selected_row.get("Ora stop", "")))
            selected_data_textbox.insert(tk.END, "Nome Inerte A: {}\n".format(selected_row.get("Nome Inerte A", "")))
            selected_data_textbox.insert(tk.END, "Dosaggio Inerte A [kg/m³]: {}\n".format(selected_row.get("Dosaggio Inerte A [kg/m³]", "")))
            selected_data_textbox.insert(tk.END, "Nome Inerte B: {}\n".format(selected_row.get("Nome Inerte B", "")))
            selected_data_textbox.insert(tk.END, "Dosaggio Inerte B [kg/m³]: {}\n".format(selected_row.get("Dosaggio Inerte B [kg/m³]", "")))
            selected_data_textbox.insert(tk.END, "Dosaggio Cemento [kg/m³]: {}\n".format(selected_row.get("Dosaggio Cemento [kg/m³]", "")))
            selected_data_textbox.insert(tk.END, "Dosaggio Acqua [l/m³]: {}\n".format(selected_row.get("Dosaggio Acqua [l/m³]", "")))
            selected_data_textbox.insert(tk.END, "Dosaggio Additivo 1 [l/m³]: {}\n".format(selected_row.get("Dosaggio Additivo 1 [l/m³]", "")))
            selected_data_textbox.insert(tk.END, "Dosaggio Additivo 2 [l/m³]: {}\n".format(selected_row.get("Dosaggio Additivo 2 [l/m³]", "")))
            selected_data_textbox.insert(tk.END, "Produzione oraria [m³/h]: {}\n".format(selected_row.get("Produzione oraria [m³/h]", "")))
            selected_data_textbox.insert(tk.END, "Var. prod.: {}\n".format(selected_row.get("Var. prod.", "")))
            selected_data_textbox.insert(tk.END, "Var. acqua: {}\n".format(selected_row.get("Var. acqua", "")))
            selected_data_textbox.insert(tk.END, "Totale correzione acqua [l]: {}\n".format(selected_row.get("Totale correzione acqua [l]", "")))
            selected_data_textbox.insert(tk.END, "Temp. ambiente start [°C]: {}\n".format(selected_row.get("Temp. ambiente start [°C]", "")))
            selected_data_textbox.insert(tk.END, "Temp. ambiente stop [°C]: {}\n".format(selected_row.get("Temp. ambiente stop [°C]", "")))

            selected_data_textbox.insert(tk.END, "\n--------------- Consumi ---------------\n")  # Separator
            
            selected_data_textbox.insert(tk.END, "Calcestruzzo [m³]: {}\n".format(selected_row.get("Calcestruzzo [m³]", "")))

            # ------------------------------------------------------------------------------------------------------------------- INERTE A
            if "Inerte A [kg]" in selected_row.index:
                teorico_inerte_a = 0  # Initialize the variable
                flag_for_kg = None  # Initialize flag for units (kg or l + kg)


                if "Compensazione Inerte A" in selected_row.index:
                    compensazione_inerte_a = selected_row["Compensazione Inerte A"]
                    if compensazione_inerte_a == "l + kg" or compensazione_inerte_a == "kg":
                        lordo_inerte_a_kg = selected_row.get("Inerte A [kg]", 0)
                        lordo_inerte_a_l_kg = selected_row.get("Acqua efficace Inerte A [l]", 0)
                        inerte_a = lordo_inerte_a_kg - lordo_inerte_a_l_kg
                        selected_data_textbox.insert(tk.END, "Inerte A [kg]: {}\n".format(inerte_a))
                        inerte_a = lordo_inerte_a_kg
                        selected_data_textbox.insert(tk.END, "Inerte A lordo [kg]: {:.2f}\n".format(inerte_a), "highlight")
                        selected_data_textbox.tag_config("highlight", background="yellow")
                        if selected_row.get("Inerte A [kg]", "") != 0:
                            umidita_eff_a = selected_row.get("Acqua efficace Inerte A [l]", 0)/inerte_a
                        else:
                            umidita_eff_a = 0
                        teorico_inerte_a = m3 * selected_row.get("Dosaggio Inerte A [kg/m³]", 0)*(1/(1-umidita_eff_a))
                        if compensazione_inerte_a == "kg":
                            flag_for_kg = "kg"
                        elif compensazione_inerte_a == "l + kg":
                            flag_for_kg = "l + kg"

                    elif compensazione_inerte_a == "l":
                        selected_data_textbox.insert(tk.END, "Inerte A [kg]: {}\n".format(selected_row.get("Inerte A [kg]", "")))
                        teorico_inerte_a = m3 * selected_row.get("Dosaggio Inerte A [kg/m³]", 0) 
                        flag_for_kg = "l (solo acqua)"
                    else:
                        selected_data_textbox.insert(tk.END, "Inerte A [kg]: {}\n".format(selected_row.get("Inerte A [kg]", "")))
                        teorico_inerte_a = m3 * selected_row.get("Dosaggio Inerte A [kg/m³]", 0)
                        flag_for_kg = "None"
                else:
                    selected_data_textbox.insert(tk.END, "Inerte A [kg]: {}\n".format(selected_row.get("Inerte A [kg]", "")))
                    teorico_inerte_a = m3* selected_row.get("Dosaggio Inerte A [kg/m³]", 0)
                    flag_for_kg = "None"

                selected_data_textbox.insert(tk.END, "Teorico Inerte A[kg]: {:.2f}\n".format(teorico_inerte_a), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

                selected_data_textbox.insert(tk.END, "Compensazione: {}\n".format(flag_for_kg), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

                # Calculate variation and percentage
                var_inerte_a = teorico_inerte_a - inerte_a
                # Calculate percentage_inerte_a only if teorico_inerte_a is not zero
                if teorico_inerte_a != 0:
                    percentage_inerte_a = (var_inerte_a / teorico_inerte_a) * 100
                else:
                    percentage_inerte_a = float('nan')  # Set percentage_inerte_a to NaN if teorico_inerte_a is zero

                # Insert the variation with percentage
                highlight_text = "Var. Inerte A [kg]: {:.2f}  ({:.2f} [%])\n".format(var_inerte_a, percentage_inerte_a)
                selected_data_textbox.insert(tk.END, highlight_text, "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")
                
            # ------------------------------------------------------------------------------------------------------------------- INERTE A2
            if "Inerte A2 [kg]" in selected_row.index:
                teorico_inerte_a2 = 0  # Initialize the variable
                flag_for_kg = None  # Initialize flag for units (kg or l + kg)

                if "Compensazione Inerte A2" in selected_row.index:
                    compensazione_inerte_a2 = selected_row["Compensazione Inerte A2"]
                    if compensazione_inerte_a2 == "l + kg" or compensazione_inerte_a2 == "kg":
                        lordo_inerte_a2_kg = selected_row.get("Inerte A2 [kg]", 0)
                        lordo_inerte_a2_l_kg = selected_row.get("Acqua efficace Inerte A2 [l]", 0)
                        inerte_a2 = lordo_inerte_a2_kg - lordo_inerte_a2_l_kg
                        selected_data_textbox.insert(tk.END, "Inerte A2 [kg]: {}\n".format(inerte_a2))
                        inerte_a2 = lordo_inerte_a2_kg
                        selected_data_textbox.insert(tk.END, "Inerte A2 lordo [kg]: {:.2f}\n".format(inerte_a2), "highlight")
                        selected_data_textbox.tag_config("highlight", background="yellow")
                        if selected_row.get("Inerte A2 [kg]", "") != 0:
                            umidita_eff_a2 = selected_row.get("Acqua efficace Inerte A2 [l]", 0)/inerte_a2
                        else:
                            umidita_eff_a2 = 0
                        teorico_inerte_a = m3 * selected_row.get("Dosaggio Inerte A2 [kg/m³]", 0)*(1/(1-umidita_eff_a2))
                        if compensazione_inerte_a2 == "kg":
                            flag_for_kg = "kg"
                        elif compensazione_inerte_a2 == "l + kg":
                            flag_for_kg = "l + kg"

                    elif compensazione_inerte_a2 == "l":
                        selected_data_textbox.insert(tk.END, "Inerte A2 [kg]: {}\n".format(selected_row.get("Inerte A2 [kg]", "")))
                        teorico_inerte_a = m3 * selected_row.get("Dosaggio Inerte A2 [kg/m³]", 0)
                        flag_for_kg = "l (solo acqua)"
                    else:
                        selected_data_textbox.insert(tk.END, "Inerte A2 [kg]: {}\n".format(selected_row.get("Inerte A2 [kg]", "")))
                        teorico_inerte_a2 = m3 * selected_row.get("Dosaggio Inerte A2 [kg/m³]", 0)
                        flag_for_kg = "None"
                else:
                    selected_data_textbox.insert(tk.END, "Inerte A2 [kg]: {}\n".format(selected_row.get("Inerte A2 [kg]", "")))
                    teorico_inerte_a2 = m3 * selected_row.get("Dosaggio Inerte A2 [kg/m³]", 0)
                    flag_for_kg = "None"

                selected_data_textbox.insert(tk.END, "Teorico Inerte A2[kg]: {:.2f}\n".format(teorico_inerte_a2), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

                selected_data_textbox.insert(tk.END, "Compensazione: {}\n".format(flag_for_kg), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")
                # Calculate variation and percentage
                var_inerte_a2 = teorico_inerte_a2 - inerte_a2
                if teorico_inerte_a2 != 0:
                    percentage_inerte_a2 = (var_inerte_a2 / teorico_inerte_a2) * 100
                else:
                    percentage_inerte_a2 = float('nan')  # Set percentage_inerte_a to NaN if teorico_inerte_a is zero

                # Insert the variation with percentage
                highlight_text = "Var. Inerte A2 [kg]: {:.2f}  ({:.2f} [%])\n".format(var_inerte_a2, percentage_inerte_a2)
                selected_data_textbox.insert(tk.END, highlight_text, "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

            # ------------------------------------------------------------------------------------------------------------------- INERTE B
            if "Inerte B [kg]" in selected_row.index:
                # Initialize teorico_inerte_b to 0
                teorico_inerte_b = 0  # Initialize the variable
                flag_for_kg = None  # Initialize flag for units (kg or l + kg)
                print(f"M3: {m3}")
                if "Compensazione Inerte B" in selected_row.index:
                    compensazione_inerte_b = selected_row["Compensazione Inerte B"]
                    
                    if compensazione_inerte_b == "l + kg" or compensazione_inerte_b == "kg":
                        lordo_inerte_b_kg = selected_row.get("Inerte B [kg]", 0)
                        lordo_inerte_b_l_kg = selected_row.get("Acqua efficace Inerte B [l]", 0)
                        inerte_b = lordo_inerte_b_kg - lordo_inerte_b_l_kg
                        selected_data_textbox.insert(tk.END, "Inerte B [kg]: {}\n".format(inerte_b))
                        inerte_b = lordo_inerte_b_kg
                        selected_data_textbox.insert(tk.END, "Inerte B lordo [kg]: {:.2f}\n".format(inerte_b), "highlight")
                        selected_data_textbox.tag_config("highlight", background="yellow")

                        if selected_row.get("Inerte B [kg]", "") != 0:
                            umidita_eff_b = selected_row.get("Acqua efficace Inerte B [l]", 0)/inerte_b
                            print(umidita_eff_b)
                        else:
                            umidita_eff_b = 0
                       
                        teorico_inerte_b = m3 * selected_row.get("Dosaggio Inerte B [kg/m³]", 0)*(1/(1-umidita_eff_b))
                        if compensazione_inerte_b == "kg":
                            flag_for_kg = "kg"
                        elif compensazione_inerte_b == "l + kg":
                            flag_for_kg = "l + kg"

                    elif compensazione_inerte_b == "l":
                        selected_data_textbox.insert(tk.END, "Inerte B [kg]: {}\n".format(selected_row.get("Inerte B [kg]", "")))
                        teorico_inerte_b = m3 * selected_row.get("Dosaggio Inerte B [kg/m³]", 0) + selected_row.get("Acqua efficace Inerte B [l]", 0)
                        flag_for_kg = "l (solo acqua)"
                    else:
                        selected_data_textbox.insert(tk.END, "Inerte B [kg]: {}\n".format(selected_row.get("Inerte B [kg]", "")))
                        teorico_inerte_b = m3 * selected_row.get("Dosaggio Inerte B [kg/m³]", 0)
                        flag_for_kg = "None"
                else:
                    selected_data_textbox.insert(tk.END, "Inerte B [kg]: {}\n".format(selected_row.get("Inerte B [kg]", "")))
                    teorico_inerte_b = m3 * selected_row.get("Dosaggio Inerte B [kg/m³]", 0)
                    flag_for_kg = "None"

                selected_data_textbox.insert(tk.END, "Teorico Inerte B[kg]: {:.2f}\n".format(teorico_inerte_b), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

                selected_data_textbox.insert(tk.END, "Compensazione: {}\n".format(flag_for_kg), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

                # Calculate variation and percentage
                var_inerte_b = teorico_inerte_b - inerte_b
                if teorico_inerte_b != 0:
                    percentage_inerte_b = (var_inerte_b / teorico_inerte_b) * 100
                else:
                    percentage_inerte_b = float('nan')  # Set percentage_inerte_a to NaN if teorico_inerte_a is zero

                # Insert the variation with percentage
                highlight_text = "Var. Inerte B [kg]: {:.2f}  ({:.2f} [%])\n".format(var_inerte_b, percentage_inerte_b)
                selected_data_textbox.insert(tk.END, highlight_text, "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")
            # ------------------------------------------------------------------------------------------------------------------- INERTE C
            if "Inerte C [kg]" in selected_row.index:
                teorico_inerte_c = 0  # Initialize the variable
                flag_for_kg = None  # Initialize flag for units (kg or l + kg)

                if "Compensazione Inerte C" in selected_row.index:
                    compensazione_inerte_c = selected_row["Compensazione Inerte C"]
                    if compensazione_inerte_c == "l + kg" or compensazione_inerte_c == "kg":
                        lordo_inerte_c_kg = selected_row.get("Inerte C [kg]", 0)
                        lordo_inerte_c_l_kg = selected_row.get("Acqua efficace Inerte C [l]", 0)
                        inerte_c = lordo_inerte_c_kg - lordo_inerte_c_l_kg
                        selected_data_textbox.insert(tk.END, "Inerte C [kg]: {}\n".format(inerte_c))
                        inerte_c = lordo_inerte_c_kg
                        selected_data_textbox.insert(tk.END, "Inerte C lordo [kg]: {:.2f}\n".format(inerte_c), "highlight")
                        selected_data_textbox.tag_config("highlight", background="yellow")
                        if selected_row.get("Inerte C [kg]", "") != 0:
                            umidita_eff_c = selected_row.get("Acqua efficace Inerte C [l]", 0)/inerte_c
                        else:
                            umidita_eff_c = 0
                        teorico_inerte_c = m3 * selected_row.get("Dosaggio Inerte C [kg/m³]", 0)*(1/(1-umidita_eff_c))
                        if compensazione_inerte_c == "kg":
                            flag_for_kg = "kg"
                        elif compensazione_inerte_c == "l + kg":
                            flag_for_kg = "l + kg"

                    elif compensazione_inerte_c == "l":
                        selected_data_textbox.insert(tk.END, "Inerte C [kg]: {}\n".format(selected_row.get("Inerte C [kg]", "")))
                        teorico_inerte_c = m3 * selected_row.get("Dosaggio Inerte C [kg/m³]", 0) + selected_row.get("Acqua efficace Inerte C [l]", 0)
                        flag_for_kg = "l (solo acqua)"
                    else:
                        selected_data_textbox.insert(tk.END, "Inerte C [kg]: {}\n".format(selected_row.get("Inerte C [kg]", "")))
                        teorico_inerte_c = m3 * selected_row.get("Dosaggio Inerte C [kg/m³]", 0)
                        flag_for_kg = "None"
                else:
                    selected_data_textbox.insert(tk.END, "Inerte C [kg]: {}\n".format(selected_row.get("Inerte C [kg]", "")))
                    teorico_inerte_c = m3 * selected_row.get("Dosaggio Inerte C [kg/m³]", 0)
                    flag_for_kg = "None"

                selected_data_textbox.insert(tk.END, "Teorico Inerte C[kg]: {:.2f}\n".format(teorico_inerte_c), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

                selected_data_textbox.insert(tk.END, "Compensazione: {}\n".format(flag_for_kg), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")
                
                # Calculate variation and percentage
                var_inerte_c = teorico_inerte_c - inerte_c
                if teorico_inerte_c != 0:
                    percentage_inerte_c = (var_inerte_c / teorico_inerte_c) * 100
                else:
                    percentage_inerte_c = float('nan')  # Set percentage_inerte_a to NaN if teorico_inerte_a is zero

                # Insert the variation with percentage
                highlight_text = "Var. Inerte C [kg]: {:.2f}  ({:.2f} [%])\n".format(var_inerte_c, percentage_inerte_c)
                selected_data_textbox.insert(tk.END, highlight_text, "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

            # ------------------------------------------------------------------------------------------------------------------- INERTE D
            if "Inerte D [kg]" in selected_row.index:
                teorico_inerte_d = 0  # Initialize the variable
                flag_for_kg = None  # Initialize flag for units (kg or l + kg)

                if "Compensazione Inerte D" in selected_row.index:
                    compensazione_inerte_d = selected_row["Compensazione Inerte D"]
                    if compensazione_inerte_d == "l + kg" or compensazione_inerte_d == "kg":
                        lordo_inerte_d_kg = selected_row.get("Inerte D [kg]", 0)
                        lordo_inerte_d_l_kg = selected_row.get("Acqua efficace Inerte D [l]", 0)
                        inerte_d = lordo_inerte_d_kg - lordo_inerte_d_l_kg
                        selected_data_textbox.insert(tk.END, "Inerte D [kg]: {}\n".format(inerte_d))
                        inerte_d = lordo_inerte_d_kg
                        selected_data_textbox.insert(tk.END, "Inerte D lordo [kg]: {:.2f}\n".format(inerte_d), "highlight")
                        selected_data_textbox.tag_config("highlight", background="yellow")
                        if selected_row.get("Inerte D [kg]", "") != 0:
                            umidita_eff_d = selected_row.get("Acqua efficace Inerte D [l]", 0)/inerte_d
                        else:
                            umidita_eff_d = 0
                        teorico_inerte_d = m3 * selected_row.get("Dosaggio Inerte D [kg/m³]", 0)*(1/(1-umidita_eff_d))
                        if compensazione_inerte_d == "kg":
                            flag_for_kg = "kg"
                        elif compensazione_inerte_d == "l + kg":
                            flag_for_kg = "l + kg"

                    elif compensazione_inerte_d == "l":
                        selected_data_textbox.insert(tk.END, "Inerte D [kg]: {}\n".format(selected_row.get("Inerte D [kg]", "")))
                        teorico_inerte_d = m3 * selected_row.get("Dosaggio Inerte D [kg/m³]", 0) + selected_row.get("Acqua efficace Inerte D [l]", 0)
                        flag_for_kg = "l (solo acqua)"
                    else:
                        selected_data_textbox.insert(tk.END, "Inerte D [kg]: {}\n".format(selected_row.get("Inerte D [kg]", "")))
                        teorico_inerte_d = m3 * selected_row.get("Dosaggio Inerte D [kg/m³]", 0)
                        flag_for_kg = "None"
                else:
                    selected_data_textbox.insert(tk.END, "Inerte D [kg]: {}\n".format(selected_row.get("Inerte D [kg]", "")))
                    teorico_inerte_d = m3 * selected_row.get("Dosaggio Inerte D [kg/m³]", 0)
                    flag_for_kg = "None"

                selected_data_textbox.insert(tk.END, "Teorico Inerte D[kg]: {:.2f}\n".format(teorico_inerte_d), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

                selected_data_textbox.insert(tk.END, "Compensazione: {}\n".format(flag_for_kg), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")
                
                # Calculate variation and percentage
                var_inerte_d = teorico_inerte_d - inerte_d
                if teorico_inerte_d != 0:
                    percentage_inerte_d = (var_inerte_d / teorico_inerte_d) * 100
                else:
                    percentage_inerte_d = float('nan')  # Set percentage_inerte_a to NaN if teorico_inerte_a is zero

                # Insert the variation with percentage
                highlight_text = "Var. Inerte D [kg]: {:.2f}  ({:.2f} [%])\n".format(var_inerte_d, percentage_inerte_d)
                selected_data_textbox.insert(tk.END, highlight_text, "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

            # ------------------------------------------------------------------------------------------------------------------- INERTE E
            if "Inerte E [kg]" in selected_row.index:
                teorico_inerte_e = 0  # Initialize the variable
                flag_for_kg = None  # Initialize flag for units (kg or l + kg)

                if "Compensazione Inerte E" in selected_row.index:
                    compensazione_inerte_e = selected_row["Compensazione Inerte E"]
                    if compensazione_inerte_e == "l + kg" or compensazione_inerte_e == "kg":
                        lordo_inerte_e_kg = selected_row.get("Inerte E [kg]", 0)
                        lordo_inerte_e_l_kg = selected_row.get("Acqua efficace Inerte E [l]", 0)
                        inerte_e = lordo_inerte_e_kg - lordo_inerte_e_l_kg
                        selected_data_textbox.insert(tk.END, "Inerte E [kg]: {}\n".format(inerte_e))
                        inerte_e = lordo_inerte_e_kg
                        selected_data_textbox.insert(tk.END, "Inerte E lordo [kg]: {:.2f}\n".format(inerte_e), "highlight")
                        selected_data_textbox.tag_config("highlight", background="yellow")
                        if selected_row.get("Inerte E [kg]", "") != 0:
                            umidita_eff_e = selected_row.get("Acqua efficace Inerte E [l]", 0)/inerte_e
                        else:
                            umidita_eff_e = 0
                        teorico_inerte_e = m3 * selected_row.get("Dosaggio Inerte E [kg/m³]", 0)*(1/(1-umidita_eff_e))
                        if compensazione_inerte_e == "kg":
                            flag_for_kg = "kg"
                        elif compensazione_inerte_e == "l + kg":
                            flag_for_kg = "l + kg"

                    elif compensazione_inerte_e == "l":
                        selected_data_textbox.insert(tk.END, "Inerte E [kg]: {}\n".format(selected_row.get("Inerte E [kg]", "")))
                        teorico_inerte_e = m3 * selected_row.get("Dosaggio Inerte E [kg/m³]", 0) + selected_row.get("Acqua efficace Inerte E [l]", 0)
                        flag_for_kg = "l (solo acqua)"
                    else:
                        selected_data_textbox.insert(tk.END, "Inerte E [kg]: {}\n".format(selected_row.get("Inerte E [kg]", "")))
                        teorico_inerte_e = m3 * selected_row.get("Dosaggio Inerte E [kg/m³]", 0)
                        flag_for_kg = "None"
                else:
                    selected_data_textbox.insert(tk.END, "Inerte E [kg]: {}\n".format(selected_row.get("Inerte E [kg]", "")))
                    teorico_inerte_e = m3 * selected_row.get("Dosaggio Inerte E [kg/m³]", 0)
                    flag_for_kg = "None"

                selected_data_textbox.insert(tk.END, "Teorico Inerte E[kg]: {:.2f}\n".format(teorico_inerte_e), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

                selected_data_textbox.insert(tk.END, "Compensazione: {}\n".format(flag_for_kg), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")
                # Calculate variation and percentage
                var_inerte_e = teorico_inerte_e - inerte_e
                if teorico_inerte_e != 0:
                    percentage_inerte_e = (var_inerte_e / teorico_inerte_e) * 100
                else:
                    percentage_inerte_e = float('nan')  # Set percentage_inerte_a to NaN if teorico_inerte_a is zero

                # Insert the variation with percentage
                highlight_text = "Var. Inerte E [kg]: {:.2f}  ({:.2f} [%])\n".format(var_inerte_e, percentage_inerte_e)
                selected_data_textbox.insert(tk.END, highlight_text, "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

            if "Cemento [kg]" in selected_row.index:
                selected_data_textbox.insert(tk.END, "Cemento [kg]: {}\n".format(selected_row.get("Cemento [kg]", "")))
                # Highlight Teorico Cement row in yellow
                teorico_cement = m3 * selected_row.get("Dosaggio Cemento [kg/m³]", 0)
                selected_data_textbox.insert(tk.END, "Teorico Cemento [kg]: {:.2f}\n".format(teorico_cement), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")
                # Calculate variation and percentage
                var_cement = teorico_cement - selected_row.get("Cemento [kg]", 0)
                if teorico_cement != 0:
                    percentage_cement = (var_cement / teorico_cement) * 100
                else:
                    percentage_cement = float('nan')  # Set percentage_inerte_a to NaN if teorico_inerte_a is zero

                # Insert the variation with percentage
                highlight_text = "Var. Cemento[kg]: {:.2f}  ({:.2f} [%])\n".format(var_cement, percentage_cement)
                selected_data_textbox.insert(tk.END, highlight_text, "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")
            
            if "Filler [kg]" in selected_row.index:
                selected_data_textbox.insert(tk.END, "Filler [kg]: {}\n".format(selected_row.get("Filler [kg]", "")))
                # Highlight Teorico Cement row in yellow
                teorico_filler = m3 * selected_row.get("Dosaggio Filler [kg/m³]", 0)
                selected_data_textbox.insert(tk.END, "Teorico Filler [kg]: {:.2f}\n".format(teorico_filler), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")
                # Calculate variation and percentage
                var_filler = teorico_filler - selected_row.get("Filler [kg]", 0)
                if teorico_filler != 0:
                    percentage_filler = (var_filler / teorico_filler) * 100
                else:
                    percentage_filler = float('nan')  # Set percentage_inerte_a to NaN if teorico_inerte_a is zero

                # Insert the variation with percentage
                highlight_text = "Var. Filler[kg]: {:.2f}  ({:.2f} [%])\n".format(var_filler, percentage_filler)
                selected_data_textbox.insert(tk.END, highlight_text, "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

            if "Acqua [l]" in selected_row.index:
                selected_data_textbox.insert(tk.END, "Acqua [l]: {}\n".format(selected_row.get("Acqua [l]", "")))
                
                # Highlight Teorico Cement row in yellow
                # Check for presence of "l" or "l + kg" in Compensazione inerte columns
                compensazione_inerte_columns = ["Compensazione Inerte A", "Compensazione Inerte B", "Compensazione Inerte C", "Compensazione Inerte D", "Compensazione Inerte E"]

                # Check if the columns exist
                if any(column in selected_row.index for column in compensazione_inerte_columns):
                    use_acqua_efficace = any("l" in str(selected_row.get(column, "")) or "l + kg" in str(selected_row.get(column, "")) for column in compensazione_inerte_columns)
                else:
                    # Handle the case where none of the columns exist
                    use_acqua_efficace = False

                # Calculate teorico_acqua based on the presence of "l" or "l + kg"
                teorico_acqua = m3 * selected_row.get("Dosaggio Acqua [l/m³]", 0)
                for column in compensazione_inerte_columns:
                    if column in selected_row.index:  # Check if the column exists in the selected row
                        if use_acqua_efficace and ("l" in str(selected_row.get(column, "")) or "l + kg" in str(selected_row.get(column, ""))):
                            teorico_acqua -= selected_row.get(f"Acqua efficace Inerte {column[-1]} [l]", 0)
                
                if "Totale correzione acqua [l]" in selected_row.index and selected_row.get("Totale correzione acqua [l]", 0) != 0:
                    teorico_acqua += selected_row.get("Totale correzione acqua [l]", 0)
                
                # Insert teorico_acqua into the textbox
                selected_data_textbox.insert(tk.END, "Teorico Acqua [l]: {:.2f}\n".format(teorico_acqua), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

                # Calculate variation and percentage
                var_acqua = teorico_acqua - selected_row.get("Acqua [l]", 0)
                if teorico_acqua != 0:
                    percentage_acqua = (var_acqua / teorico_acqua) * 100
                else:
                    percentage_acqua = float('nan')  # Set percentage_inerte_a to NaN if teorico_inerte_a is zero

                # Insert the variation with percentage
                highlight_text = "Var. Acqua [l]: {:.2f}  ({:.2f} [%])\n".format(var_acqua, percentage_acqua)
                selected_data_textbox.insert(tk.END, highlight_text, "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")


            if "Additivo 1 [l]" in selected_row.index:
                # Insert the actual value of Additive 1
                selected_data_textbox.insert(tk.END, "Additivo 1 [l]: {}\n".format(selected_row.get("Additivo 1 [l]", "")))

                # Calculate the theoretical value for Additive 1
                teorico_ad1 = m3 * selected_row.get("Dosaggio Additivo 1 [l/m³]", 0)
                selected_data_textbox.insert(tk.END, "Teorico Additivo 1 [l]: {:.2f}\n".format(teorico_ad1), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

                # Get the actual value for Additive 1 and calculate the variation
                lordo_ad1 = selected_row.get("Additivo 1 [l]", 0)  # Safe access to avoid KeyError
                var_ad1 = round(teorico_ad1 - lordo_ad1, 2)  # Calculate variation and round to 2 decimals

                # Calculate the percentage variation only if the theoretical value is not zero
                if teorico_ad1 != 0:
                    percentage_ad1 = round((var_ad1 / teorico_ad1) * 100, 2)  # Calculate and round to 2 decimals
                else:
                    percentage_ad1 = float('nan')  # Set to NaN if the theoretical value is zero

                # Insert the variation and percentage with highlighting
                highlight_text = "Var. Additivo 1 [l]: {:.2f}  ({:.2f} [%])\n".format(var_ad1, percentage_ad1)
                selected_data_textbox.insert(tk.END, highlight_text, "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")


            if "Additivo 2 [l]" in selected_row.index:
                ad2 = 0  # Initialize the actual additive value
                selected_data_textbox.insert(tk.END, "Additivo 2 [l]: {}\n".format(selected_row.get("Additivo 2 [l]", "")))

                # Calculate the theoretical value for Additive 2
                teorico_ad2 = m3 * selected_row.get("Dosaggio Additivo 2 [l/m³]", 0)
                selected_data_textbox.insert(tk.END, "Teorico Additivo 2 [l]: {:.2f}\n".format(teorico_ad2), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

                # Get the actual value for Additive 2 and calculate the variation
                ad2 = selected_row.get("Additivo 2 [l]", 0)
                var_ad2 = round(teorico_ad2 - ad2, 2)  # Calculate and round to 2 decimals

                # Calculate the percentage variation only if the theoretical value is not zero
                if teorico_ad2 != 0:
                    percentage_ad2 = round((var_ad2 / teorico_ad2) * 100, 2)  # Calculate and round to 2 decimals
                else:
                    percentage_ad2 = float('nan')  # Set to NaN if the theoretical value is zero

                # Insert the variation and percentage with highlighting
                highlight_text = "Var. Additivo 2 [l]: {:.2f}  ({:.2f} [%])\n".format(var_ad2, percentage_ad2)
                selected_data_textbox.insert(tk.END, highlight_text, "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")


            if "Additivo 3 [l]" in selected_row.index:
                # Insert the actual value of Additive 3
                selected_data_textbox.insert(tk.END, "Additivo 3 [l]: {}\n".format(selected_row.get("Additivo 3 [l]", "")))

                # Calculate the theoretical value for Additive 3
                teorico_ad3 = m3 * selected_row.get("Dosaggio Additivo 3 [l/m³]", 0)
                selected_data_textbox.insert(tk.END, "Teorico Additivo 3 [l]: {:.2f}\n".format(teorico_ad3), "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")

                # Get the actual value for Additive 3 and calculate the variation
                lordo_ad3 = selected_row.get("Additivo 3 [l]", 0)  # Safe access to avoid KeyError
                var_ad3 = round(teorico_ad3 - lordo_ad3, 2)  # Calculate variation and round to 2 decimals

                # Calculate the percentage variation only if the theoretical value is not zero
                if teorico_ad3 != 0:
                    percentage_ad3 = round((var_ad3 / teorico_ad3) * 100, 2)  # Calculate and round to 2 decimals
                else:
                    percentage_ad3 = float('nan')  # Set to NaN if the theoretical value is zero

                # Insert the variation and percentage with highlighting
                highlight_text = "Var. Additivo 3 [l]: {:.2f}  ({:.2f} [%])\n".format(var_ad3, percentage_ad3)
                selected_data_textbox.insert(tk.END, highlight_text, "highlight")
                selected_data_textbox.tag_config("highlight", background="yellow")


            selected_data_textbox.insert(tk.END, "Umidità Inerte A [%]: {}\n".format(selected_row.get("Umidità Inerte A [%]", "")))
            selected_data_textbox.insert(tk.END, "Assorbimento inerte A [%]: {}\n".format(selected_row.get("Assorbimento inerte A [%]", "")))
            selected_data_textbox.insert(tk.END, "Acqua efficace Inerte A [l]: {}\n".format(selected_row.get("Acqua efficace Inerte A [l]", "")))
            selected_data_textbox.insert(tk.END, "Umidità Inerte B [%]: {}\n".format(selected_row.get("Umidità Inerte B [%]", "")))
            selected_data_textbox.insert(tk.END, "Assorbimento inerte B [%]: {}\n".format(selected_row.get("Assorbimento inerte B [%]", "")))
            selected_data_textbox.insert(tk.END, "Acqua efficace Inerte B [l]: {}\n".format(selected_row.get("Acqua efficace Inerte B [l]", "")))
            selected_data_textbox.insert(tk.END, "Acqua/Cemento [l/kg]: {}\n".format(selected_row.get("Acqua/Cemento [l/kg]", "")))
            selected_data_textbox.insert(tk.END, "Cemento - Peso netto in silo [kg]: {}\n".format(selected_row.get("Cemento - Peso netto in silo [kg]", "")))
            

            selected_data_textbox.insert(tk.END, "\n--------------- Alarmi ---------------\n")  # Separator for alarms
            # Handle alarms
            alarms = [selected_row.get(f"Allarme {i}", "") for i in range(1, 33)]
            selected_alarms = [alarm for alarm in alarms if pd.notna(alarm) and alarm != ""]
            if selected_alarms:
                for alarm in selected_alarms:
                    selected_data_textbox.insert(tk.END, "- {}\n".format(alarm))

    except Exception as e:
        messagebox.showerror("Error", f"3 An error occurred while updating selected data: {str(e)}")
                
# Function to open a new tab in the browser to display the selected data in columns
def open_selected_data_in_browser():
    try:
        selected_indexes = ora_listbox.curselection()
        selected_date = pd.to_datetime(date_combobox.get(), dayfirst=True).date()  # Convert selected date to yyyy-mm-dd format
        filtered_data = bb_op_data[pd.to_datetime(bb_op_data["Ora start"], dayfirst=True).dt.date == selected_date]

        # Inicializar listas para almacenar resultados de cálculos
        lordo_inerte_a_l_kg_values = []
        lordo_inerte_a_kg_values = []
        lordo_inerte_a_values = []
        teorico_inerte_a_values = []
        compensazione_valuesa = []
        var_inerte_a_values = []
        percentage_inerte_a_values = []
        lordo_inerte_a2_l_kg_values = []
        lordo_inerte_a2_kg_values = []
        lordo_inerte_a2_values = []
        teorico_inerte_a2_values = []
        compensazione_valuesa2 = []
        var_inerte_a2_values = []
        percentage_inerte_a2_values = []
        lordo_inerte_b_l_kg_values = []
        lordo_inerte_b_kg_values = []
        lordo_inerte_b_values = []
        teorico_inerte_b_values = []
        compensazione_valuesb = []
        var_inerte_b_values = []
        percentage_inerte_b_values = []
        lordo_inerte_c_l_kg_values = []
        lordo_inerte_c_kg_values = []
        lordo_inerte_c_values = []
        teorico_inerte_c_values = []
        compensazione_valuesc = []
        var_inerte_c_values = []
        percentage_inerte_c_values = []
        lordo_inerte_d_l_kg_values = []
        lordo_inerte_d_kg_values = []
        lordo_inerte_d_values = []
        teorico_inerte_d_values = []
        compensazione_valuesd = []
        var_inerte_d_values = []
        percentage_inerte_d_values = []
        lordo_inerte_e_l_kg_values = []
        lordo_inerte_e_kg_values = []
        lordo_inerte_e_values = []
        teorico_inerte_e_values = []
        compensazione_valuese = []
        var_inerte_e_values = []
        percentage_inerte_e_values = []

        inerte_a = inerte_a2 = inerte_b = inerte_c = inerte_d = inerte_e = dosaggio = 0
        inerte_list = ["A", "A2", "B", "C", "D", "E"]

        for index in selected_indexes:
            # Reiniciar variables locales al inicio de cada iteración
            inerte_a = inerte_a2 = inerte_b = inerte_c = inerte_d = inerte_e = 0
            dosaggio = 0

            selected_row = filtered_data.iloc[index]

                    # Calcular valores de todos los inertes y dosaggio
            for inerte in inerte_list:
                if selected_row.get(f"Inerte {inerte[-1]} [kg]", 0):
                    inerte_kg = selected_row.get(f"Inerte {inerte[-1]} [kg]", 0)
                    acqua_eff = selected_row.get(f"Acqua efficace Inerte {inerte[-1]} [l]", 0)
                    umedita_eff = acqua_eff / inerte_kg if inerte_kg != 0 else 0
                    if inerte == "A":
                        inerte_a = inerte_kg * (1 - umedita_eff)
                        if inerte_a != 0:
                            dosaggio += selected_row.get("Dosaggio Inerte A [kg/m³]", 0)
                    elif inerte == "A2":
                        inerte_a2 = inerte_kg * (1 - umedita_eff)
                        if inerte_a2 != 0:
                            dosaggio += selected_row.get("Dosaggio Inerte A2 [kg/m³]", 0)
                    elif inerte == "B":
                        inerte_b = inerte_kg * (1 - umedita_eff)
                        if inerte_b != 0:
                            dosaggio += selected_row.get("Dosaggio Inerte B [kg/m³]", 0)
                    elif inerte == "C":
                        inerte_c = inerte_kg * (1 - umedita_eff)
                        if inerte_c != 0:
                            dosaggio += selected_row.get("Dosaggio Inerte C [kg/m³]", 0)
                    elif inerte == "D":
                        inerte_d = inerte_kg * (1 - umedita_eff)
                        if inerte_d != 0:
                            dosaggio += selected_row.get("Dosaggio Inerte D [kg/m³]", 0)
                    elif inerte == "E":
                        inerte_e = inerte_kg * (1 - umedita_eff)
                        if inerte_e != 0:
                            dosaggio += selected_row.get("Dosaggio Inerte E [kg/m³]", 0)

            # Calcular m3
            m3 = inerte_a + inerte_a2 + inerte_b + inerte_c + inerte_d + inerte_e
            if dosaggio != 0:
                m3 = m3 / dosaggio
            else:
                for dosaggio_col in ["Dosaggio Cemento [kg/m³]", "Dosaggio Acqua [l/m³]", "Dosaggio Additivo 1 [l/m³]", "Dosaggio Additivo 2 [l/m³]", "Dosaggio Additivo 3 [l/m³]"]:
                    if selected_row.get(dosaggio_col, 0) != 0:
                        m3 = m3 / selected_row.get(dosaggio_col, 0)
                        break

            # Cálculo de valores específicos para "Inerte A"
            teorico_inerte_a = 0
            flag_for_kg = None
            lordo_inerte_a_kg = selected_row.get("Inerte A [kg]", 0)
            lordo_inerte_a_l_kg = selected_row.get("Acqua efficace Inerte A [l]", 0)

            if "Compensazione Inerte A" in selected_row.index:
                compensazione_inerte_a = selected_row["Compensazione Inerte A"]
                if compensazione_inerte_a == "l + kg" or compensazione_inerte_a == "kg":
                    inerte_a = lordo_inerte_a_kg - lordo_inerte_a_l_kg
                    inerte_a = lordo_inerte_a_kg  # Restablecer a valor original para otros cálculos

                    if inerte_a != 0:
                        umidita_eff_a = lordo_inerte_a_l_kg / inerte_a
                    else:
                        umidita_eff_a = 0

                    teorico_inerte_a = m3 * selected_row.get("Dosaggio Inerte A [kg/m³]", 0) * (1 / (1 - umidita_eff_a))
                    flag_for_kg = "kg" if compensazione_inerte_a == "kg" else "l + kg"

                elif compensazione_inerte_a == "l":
                    teorico_inerte_a = m3 * selected_row.get("Dosaggio Inerte A [kg/m³]", 0)
                    flag_for_kg = "l (solo acqua)"
                else:
                    teorico_inerte_a = m3 * selected_row.get("Dosaggio Inerte A [kg/m³]", 0)
                    flag_for_kg = "None"
            else:
                teorico_inerte_a = m3 * selected_row.get("Dosaggio Inerte A [kg/m³]", 0)
                flag_for_kg = "None"

            # Guardar resultados para la tabla HTML
            lordo_inerte_a_l_kg_values.append(lordo_inerte_a_l_kg)
            lordo_inerte_a_kg_values.append(lordo_inerte_a_kg)
            lordo_inerte_a_values.append(lordo_inerte_a_kg + lordo_inerte_a_l_kg)
            teorico_inerte_a_values.append(teorico_inerte_a)
            compensazione_valuesa.append(flag_for_kg)

            # Calcular variación y porcentaje
            var_inerte_a = teorico_inerte_a - inerte_a
            if teorico_inerte_a != 0:
                percentage_inerte_a = (var_inerte_a / teorico_inerte_a) * 100
            else:
                percentage_inerte_a = float('nan')

            var_inerte_a_values.append(var_inerte_a)
            percentage_inerte_a_values.append(percentage_inerte_a)

        # -------------------------------------------------------------------------------------- INERTE A2
        for index in selected_indexes:
            # Reinicializar las variables locales al inicio de cada iteración
            selected_row = filtered_data.iloc[index]
            teorico_inerte_a2 = 0
            flag_for_kg = None
            inerte_a2 = 0  # Reiniciar el valor de inerte_a2 en cada iteración

            # Calcular m3 individualmente para cada iteración
            inerte_a_iter = selected_row.get("Inerte A [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A [l]", 0) / selected_row.get("Inerte A [kg]", 0) if selected_row.get("Inerte A [kg]", 0) != 0 else 0))
            inerte_a2_iter = selected_row.get("Inerte A2 [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A2 [l]", 0) / selected_row.get("Inerte A2 [kg]", 0) if selected_row.get("Inerte A2 [kg]", 0) != 0 else 0))
            inerte_b_iter = selected_row.get("Inerte B [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte B [l]", 0) / selected_row.get("Inerte B [kg]", 0) if selected_row.get("Inerte B [kg]", 0) != 0 else 0))
            inerte_c_iter = selected_row.get("Inerte C [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte C [l]", 0) / selected_row.get("Inerte C [kg]", 0) if selected_row.get("Inerte C [kg]", 0) != 0 else 0))
            inerte_d_iter = selected_row.get("Inerte D [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte D [l]", 0) / selected_row.get("Inerte D [kg]", 0) if selected_row.get("Inerte D [kg]", 0) != 0 else 0))
            inerte_e_iter = selected_row.get("Inerte E [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte E [l]", 0) / selected_row.get("Inerte E [kg]", 0) if selected_row.get("Inerte E [kg]", 0) != 0 else 0))

            m3_iter = inerte_a_iter + inerte_a2_iter + inerte_b_iter + inerte_c_iter + inerte_d_iter + inerte_e_iter
            dosaggio_iter = sum([selected_row.get(f"Dosaggio Inerte {inerte} [kg/m³]", 0) for inerte in "A A2 B C D E".split()])
            if dosaggio_iter != 0:
                m3_iter /= dosaggio_iter
            else:
                for dosaggio_col in ["Dosaggio Cemento [kg/m³]", "Dosaggio Acqua [l/m³]", "Dosaggio Additivo 1 [l/m³]", "Dosaggio Additivo 2 [l/m³]", "Dosaggio Additivo 3 [l/m³]"]:
                    if selected_row.get(dosaggio_col, 0) != 0:
                        m3_iter /= selected_row.get(dosaggio_col, 0)
                        break

            # Obtener los valores actuales de la fila
            lordo_inerte_a2_kg = selected_row.get("Inerte A2 [kg]", 0)
            lordo_inerte_a2_l_kg = selected_row.get("Acqua efficace Inerte A2 [l]", 0)

            # Verificar la compensación de "Inerte A2"
            if "Compensazione Inerte A2" in selected_row.index:
                compensazione_inerte_a2 = selected_row["Compensazione Inerte A2"]
                if compensazione_inerte_a2 == "l + kg" or compensazione_inerte_a2 == "kg":
                    if lordo_inerte_a2_kg != 0:
                        # Calcular el valor neto y el teórico de "Inerte A2"
                        inerte_a2 = lordo_inerte_a2_kg - lordo_inerte_a2_l_kg
                        umidita_eff_a2 = lordo_inerte_a2_l_kg / inerte_a2 if inerte_a2 != 0 else 0
                        teorico_inerte_a2 = m3_iter * selected_row.get("Dosaggio Inerte A2 [kg/m³]", 0) * (1 / (1 - umidita_eff_a2))
                        flag_for_kg = "kg" if compensazione_inerte_a2 == "kg" else "l + kg"
                    else:
                        umidita_eff_a2 = 0
                        teorico_inerte_a2 = m3_iter * selected_row.get("Dosaggio Inerte A2 [kg/m³]", 0)
                elif compensazione_inerte_a2 == "l":
                    teorico_inerte_a2 = m3_iter * selected_row.get("Dosaggio Inerte A2 [kg/m³]", 0)
                    flag_for_kg = "l (solo acqua)"
                else:
                    teorico_inerte_a2 = m3_iter * selected_row.get("Dosaggio Inerte A2 [kg/m³]", 0)
                    flag_for_kg = "None"
            else:
                teorico_inerte_a2 = m3_iter * selected_row.get("Dosaggio Inerte A2 [kg/m³]", 0)
                flag_for_kg = "None"

            # Guardar resultados en listas para la producción actual
            lordo_inerte_a2_l_kg_values.append(lordo_inerte_a2_l_kg)
            lordo_inerte_a2_kg_values.append(lordo_inerte_a2_kg)
            lordo_inerte_a2_values.append(lordo_inerte_a2_kg + lordo_inerte_a2_l_kg)
            teorico_inerte_a2_values.append(teorico_inerte_a2)
            compensazione_valuesa2.append(flag_for_kg)

            # Calcular variación y porcentaje para la producción actual
            var_inerte_a2 = teorico_inerte_a2 - lordo_inerte_a2_kg  # Usar el valor bruto para la variación
            if teorico_inerte_a2 != 0:
                percentage_inerte_a2 = (var_inerte_a2 / teorico_inerte_a2) * 100
            else:
                percentage_inerte_a2 = float('nan')

            var_inerte_a2_values.append(var_inerte_a2)
            percentage_inerte_a2_values.append(percentage_inerte_a2)



        # -------------------------------------------------------------------------------------- INERTE B
        for index in selected_indexes:
            # Reinicializar las variables locales al inicio de cada iteración
            selected_row = filtered_data.iloc[index]
            teorico_inerte_b = 0
            flag_for_kg = None
            inerte_b = 0  # Reiniciar el valor de inerte_b en cada iteración

            # Calcular m3 individualmente para cada iteración
            inerte_a_iter = selected_row.get("Inerte A [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A [l]", 0) / selected_row.get("Inerte A [kg]", 0) if selected_row.get("Inerte A [kg]", 0) != 0 else 0))
            inerte_a2_iter = selected_row.get("Inerte A2 [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A2 [l]", 0) / selected_row.get("Inerte A2 [kg]", 0) if selected_row.get("Inerte A2 [kg]", 0) != 0 else 0))
            inerte_b_iter = selected_row.get("Inerte B [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte B [l]", 0) / selected_row.get("Inerte B [kg]", 0) if selected_row.get("Inerte B [kg]", 0) != 0 else 0))
            inerte_c_iter = selected_row.get("Inerte C [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte C [l]", 0) / selected_row.get("Inerte C [kg]", 0) if selected_row.get("Inerte C [kg]", 0) != 0 else 0))
            inerte_d_iter = selected_row.get("Inerte D [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte D [l]", 0) / selected_row.get("Inerte D [kg]", 0) if selected_row.get("Inerte D [kg]", 0) != 0 else 0))
            inerte_e_iter = selected_row.get("Inerte E [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte E [l]", 0) / selected_row.get("Inerte E [kg]", 0) if selected_row.get("Inerte E [kg]", 0) != 0 else 0))

            m3_iter = inerte_a_iter + inerte_a2_iter + inerte_b_iter + inerte_c_iter + inerte_d_iter + inerte_e_iter
            dosaggio_iter = sum([selected_row.get(f"Dosaggio Inerte {inerte} [kg/m³]", 0) for inerte in "A A2 B C D E".split()])
            if dosaggio_iter != 0:
                m3_iter /= dosaggio_iter
            else:
                for dosaggio_col in ["Dosaggio Cemento [kg/m³]", "Dosaggio Acqua [l/m³]", "Dosaggio Additivo 1 [l/m³]", "Dosaggio Additivo 2 [l/m³]", "Dosaggio Additivo 3 [l/m³]"]:
                    if selected_row.get(dosaggio_col, 0) != 0:
                        m3_iter /= selected_row.get(dosaggio_col, 0)
                        break

            # Obtener los valores actuales de la fila
            lordo_inerte_b_kg = selected_row.get("Inerte B [kg]", 0)
            lordo_inerte_b_l_kg = selected_row.get("Acqua efficace Inerte B [l]", 0)

            # Verificar la compensación de "Inerte B"
            if "Compensazione Inerte B" in selected_row.index:
                compensazione_inerte_b = selected_row["Compensazione Inerte B"]
                if compensazione_inerte_b == "l + kg" or compensazione_inerte_b == "kg":
                    if lordo_inerte_b_kg != 0:
                        # Calcular el valor neto y el teórico de "Inerte B"
                        inerte_b = lordo_inerte_b_kg - lordo_inerte_b_l_kg
                        umidita_eff_b = lordo_inerte_b_l_kg / inerte_b if inerte_b != 0 else 0
                        teorico_inerte_b = m3_iter * selected_row.get("Dosaggio Inerte B [kg/m³]", 0) * (1 / (1 - umidita_eff_b))
                        flag_for_kg = "kg" if compensazione_inerte_b == "kg" else "l + kg"
                    else:
                        umidita_eff_b = 0
                        teorico_inerte_b = m3_iter * selected_row.get("Dosaggio Inerte B [kg/m³]", 0)
                elif compensazione_inerte_b == "l":
                    teorico_inerte_b = m3_iter * selected_row.get("Dosaggio Inerte B [kg/m³]", 0)
                    flag_for_kg = "l (solo acqua)"
                else:
                    teorico_inerte_b = m3_iter * selected_row.get("Dosaggio Inerte B [kg/m³]", 0)
                    flag_for_kg = "None"
            else:
                teorico_inerte_b = m3_iter * selected_row.get("Dosaggio Inerte B [kg/m³]", 0)
                flag_for_kg = "None"

            # Guardar resultados en listas para la producción actual
            lordo_inerte_b_l_kg_values.append(lordo_inerte_b_l_kg)
            lordo_inerte_b_kg_values.append(lordo_inerte_b_kg)
            lordo_inerte_b_values.append(lordo_inerte_b_kg + lordo_inerte_b_l_kg)
            teorico_inerte_b_values.append(teorico_inerte_b)
            compensazione_valuesb.append(flag_for_kg)

            # Calcular variación y porcentaje para la producción actual
            var_inerte_b = teorico_inerte_b - lordo_inerte_b_kg  # Usar el valor bruto para la variación
            if teorico_inerte_b != 0:
                percentage_inerte_b = (var_inerte_b / teorico_inerte_b) * 100
            else:
                percentage_inerte_b = float('nan')

            var_inerte_b_values.append(var_inerte_b)
            percentage_inerte_b_values.append(percentage_inerte_b)



        # -------------------------------------------------------------------------------------- INERTE C
        for index in selected_indexes:
            # Reinicializar las variables locales al inicio de cada iteración
            selected_row = filtered_data.iloc[index]
            teorico_inerte_c = 0
            flag_for_kg = None
            inerte_c = 0  # Reiniciar el valor de inerte_c en cada iteración

            # Calcular m3 individualmente para cada iteración
            inerte_a_iter = selected_row.get("Inerte A [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A [l]", 0) / selected_row.get("Inerte A [kg]", 0) if selected_row.get("Inerte A [kg]", 0) != 0 else 0))
            inerte_a2_iter = selected_row.get("Inerte A2 [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A2 [l]", 0) / selected_row.get("Inerte A2 [kg]", 0) if selected_row.get("Inerte A2 [kg]", 0) != 0 else 0))
            inerte_b_iter = selected_row.get("Inerte B [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte B [l]", 0) / selected_row.get("Inerte B [kg]", 0) if selected_row.get("Inerte B [kg]", 0) != 0 else 0))
            inerte_c_iter = selected_row.get("Inerte C [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte C [l]", 0) / selected_row.get("Inerte C [kg]", 0) if selected_row.get("Inerte C [kg]", 0) != 0 else 0))
            inerte_d_iter = selected_row.get("Inerte D [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte D [l]", 0) / selected_row.get("Inerte D [kg]", 0) if selected_row.get("Inerte D [kg]", 0) != 0 else 0))
            inerte_e_iter = selected_row.get("Inerte E [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte E [l]", 0) / selected_row.get("Inerte E [kg]", 0) if selected_row.get("Inerte E [kg]", 0) != 0 else 0))

            m3_iter = inerte_a_iter + inerte_a2_iter + inerte_b_iter + inerte_c_iter + inerte_d_iter + inerte_e_iter
            dosaggio_iter = sum([selected_row.get(f"Dosaggio Inerte {inerte} [kg/m³]", 0) for inerte in "A A2 B C D E".split()])
            if dosaggio_iter != 0:
                m3_iter /= dosaggio_iter
            else:
                for dosaggio_col in ["Dosaggio Cemento [kg/m³]", "Dosaggio Acqua [l/m³]", "Dosaggio Additivo 1 [l/m³]", "Dosaggio Additivo 2 [l/m³]", "Dosaggio Additivo 3 [l/m³]"]:
                    if selected_row.get(dosaggio_col, 0) != 0:
                        m3_iter /= selected_row.get(dosaggio_col, 0)
                        break

            # Obtener los valores actuales de la fila
            lordo_inerte_c_kg = selected_row.get("Inerte C [kg]", 0)
            lordo_inerte_c_l_kg = selected_row.get("Acqua efficace Inerte C [l]", 0)

            # Verificar la compensación de "Inerte C"
            if "Compensazione Inerte C" in selected_row.index:
                compensazione_inerte_c = selected_row["Compensazione Inerte C"]
                if compensazione_inerte_c == "l + kg" or compensazione_inerte_c == "kg":
                    if lordo_inerte_c_kg != 0:
                        # Calcular el valor neto y el teórico de "Inerte C"
                        inerte_c = lordo_inerte_c_kg - lordo_inerte_c_l_kg
                        umidita_eff_c = lordo_inerte_c_l_kg / inerte_c if inerte_c != 0 else 0
                        teorico_inerte_c = m3_iter * selected_row.get("Dosaggio Inerte C [kg/m³]", 0) * (1 / (1 - umidita_eff_c))
                        flag_for_kg = "kg" if compensazione_inerte_c == "kg" else "l + kg"
                    else:
                        umidita_eff_c = 0
                        teorico_inerte_c = m3_iter * selected_row.get("Dosaggio Inerte C [kg/m³]", 0)
                elif compensazione_inerte_c == "l":
                    teorico_inerte_c = m3_iter * selected_row.get("Dosaggio Inerte C [kg/m³]", 0)
                    flag_for_kg = "l (only water)"
                else:
                    teorico_inerte_c = m3_iter * selected_row.get("Dosaggio Inerte C [kg/m³]", 0)
                    flag_for_kg = "None"
            else:
                teorico_inerte_c = m3_iter * selected_row.get("Dosaggio Inerte C [kg/m³]", 0)
                flag_for_kg = "None"

            # Guardar resultados en listas para la producción actual
            lordo_inerte_c_l_kg_values.append(lordo_inerte_c_l_kg)
            lordo_inerte_c_kg_values.append(lordo_inerte_c_kg)
            lordo_inerte_c_values.append(lordo_inerte_c_kg + lordo_inerte_c_l_kg)
            teorico_inerte_c_values.append(teorico_inerte_c)
            compensazione_valuesc.append(flag_for_kg)

            # Calcular variación y porcentaje para la producción actual
            var_inerte_c = teorico_inerte_c - lordo_inerte_c_kg  # Usar el valor bruto para la variación
            if teorico_inerte_c != 0:
                percentage_inerte_c = (var_inerte_c / teorico_inerte_c) * 100
            else:
                percentage_inerte_c = float('nan')

            var_inerte_c_values.append(var_inerte_c)
            percentage_inerte_c_values.append(percentage_inerte_c)

                
        # -------------------------------------------------------------------------------------- INERTE D
        for index in selected_indexes:
            # Reinicializar las variables locales al inicio de cada iteración
            selected_row = filtered_data.iloc[index]
            teorico_inerte_d = 0
            flag_for_kg = None
            inerte_d = 0  # Reiniciar el valor de inerte_d en cada iteración

            # Calcular m3 individualmente para cada iteración
            inerte_a_iter = selected_row.get("Inerte A [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A [l]", 0) / selected_row.get("Inerte A [kg]", 0) if selected_row.get("Inerte A [kg]", 0) != 0 else 0))
            inerte_a2_iter = selected_row.get("Inerte A2 [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A2 [l]", 0) / selected_row.get("Inerte A2 [kg]", 0) if selected_row.get("Inerte A2 [kg]", 0) != 0 else 0))
            inerte_b_iter = selected_row.get("Inerte B [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte B [l]", 0) / selected_row.get("Inerte B [kg]", 0) if selected_row.get("Inerte B [kg]", 0) != 0 else 0))
            inerte_c_iter = selected_row.get("Inerte C [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte C [l]", 0) / selected_row.get("Inerte C [kg]", 0) if selected_row.get("Inerte C [kg]", 0) != 0 else 0))
            inerte_d_iter = selected_row.get("Inerte D [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte D [l]", 0) / selected_row.get("Inerte D [kg]", 0) if selected_row.get("Inerte D [kg]", 0) != 0 else 0))
            inerte_e_iter = selected_row.get("Inerte E [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte E [l]", 0) / selected_row.get("Inerte E [kg]", 0) if selected_row.get("Inerte E [kg]", 0) != 0 else 0))

            m3_iter = inerte_a_iter + inerte_a2_iter + inerte_b_iter + inerte_c_iter + inerte_d_iter + inerte_e_iter
            dosaggio_iter = sum([selected_row.get(f"Dosaggio Inerte {inerte} [kg/m³]", 0) for inerte in "A A2 B C D E".split()])
            if dosaggio_iter != 0:
                m3_iter /= dosaggio_iter
            else:
                for dosaggio_col in ["Dosaggio Cemento [kg/m³]", "Dosaggio Acqua [l/m³]", "Dosaggio Additivo 1 [l/m³]", "Dosaggio Additivo 2 [l/m³]", "Dosaggio Additivo 3 [l/m³]"]:
                    if selected_row.get(dosaggio_col, 0) != 0:
                        m3_iter /= selected_row.get(dosaggio_col, 0)
                        break

            # Obtener los valores actuales de la fila
            lordo_inerte_d_kg = selected_row.get("Inerte D [kg]", 0)
            lordo_inerte_d_l_kg = selected_row.get("Acqua efficace Inerte D [l]", 0)

            # Verificar la compensación de "Inerte D"
            if "Compensazione Inerte D" in selected_row.index:
                compensazione_inerte_d = selected_row["Compensazione Inerte D"]
                if compensazione_inerte_d == "l + kg" or compensazione_inerte_d == "kg":
                    if lordo_inerte_d_kg != 0:
                        # Calcular el valor neto y el teórico de "Inerte D"
                        inerte_d = lordo_inerte_d_kg - lordo_inerte_d_l_kg
                        umidita_eff_d = lordo_inerte_d_l_kg / inerte_d if inerte_d != 0 else 0
                        teorico_inerte_d = m3_iter * selected_row.get("Dosaggio Inerte D [kg/m³]", 0) * (1 / (1 - umidita_eff_d))
                        flag_for_kg = "kg" if compensazione_inerte_d == "kg" else "l + kg"
                    else:
                        umidita_eff_d = 0
                        teorico_inerte_d = m3_iter * selected_row.get("Dosaggio Inerte D [kg/m³]", 0)
                elif compensazione_inerte_d == "l":
                    teorico_inerte_d = m3_iter * selected_row.get("Dosaggio Inerte D [kg/m³]", 0)
                    flag_for_kg = "l (only water)"
                else:
                    teorico_inerte_d = m3_iter * selected_row.get("Dosaggio Inerte D [kg/m³]", 0)
                    flag_for_kg = "None"
            else:
                teorico_inerte_d = m3_iter * selected_row.get("Dosaggio Inerte D [kg/m³]", 0)
                flag_for_kg = "None"

            # Guardar resultados en listas para la producción actual
            lordo_inerte_d_l_kg_values.append(lordo_inerte_d_l_kg)
            lordo_inerte_d_kg_values.append(lordo_inerte_d_kg)
            lordo_inerte_d_values.append(lordo_inerte_d_kg + lordo_inerte_d_l_kg)
            teorico_inerte_d_values.append(teorico_inerte_d)
            compensazione_valuesd.append(flag_for_kg)

            # Calcular variación y porcentaje para la producción actual
            var_inerte_d = teorico_inerte_d - lordo_inerte_d_kg  # Usar el valor bruto para la variación
            if teorico_inerte_d != 0:
                percentage_inerte_d = (var_inerte_d / teorico_inerte_d) * 100
            else:
                percentage_inerte_d = float('nan')

            var_inerte_d_values.append(var_inerte_d)
            percentage_inerte_d_values.append(percentage_inerte_d)



        # -------------------------------------------------------------------------------------- INERTE E
        for index in selected_indexes:
            # Reinicializar las variables locales al inicio de cada iteración
            selected_row = filtered_data.iloc[index]
            teorico_inerte_e = 0
            flag_for_kg = None
            inerte_e = 0  # Reiniciar el valor de inerte_e en cada iteración

            # Calcular m3 individualmente para cada iteración
            inerte_a_iter = selected_row.get("Inerte A [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A [l]", 0) / selected_row.get("Inerte A [kg]", 0) if selected_row.get("Inerte A [kg]", 0) != 0 else 0))
            inerte_a2_iter = selected_row.get("Inerte A2 [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A2 [l]", 0) / selected_row.get("Inerte A2 [kg]", 0) if selected_row.get("Inerte A2 [kg]", 0) != 0 else 0))
            inerte_b_iter = selected_row.get("Inerte B [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte B [l]", 0) / selected_row.get("Inerte B [kg]", 0) if selected_row.get("Inerte B [kg]", 0) != 0 else 0))
            inerte_c_iter = selected_row.get("Inerte C [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte C [l]", 0) / selected_row.get("Inerte C [kg]", 0) if selected_row.get("Inerte C [kg]", 0) != 0 else 0))
            inerte_d_iter = selected_row.get("Inerte D [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte D [l]", 0) / selected_row.get("Inerte D [kg]", 0) if selected_row.get("Inerte D [kg]", 0) != 0 else 0))
            inerte_e_iter = selected_row.get("Inerte E [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte E [l]", 0) / selected_row.get("Inerte E [kg]", 0) if selected_row.get("Inerte E [kg]", 0) != 0 else 0))

            m3_iter = inerte_a_iter + inerte_a2_iter + inerte_b_iter + inerte_c_iter + inerte_d_iter + inerte_e_iter
            dosaggio_iter = sum([selected_row.get(f"Dosaggio Inerte {inerte} [kg/m³]", 0) for inerte in "A A2 B C D E".split()])
            if dosaggio_iter != 0:
                m3_iter /= dosaggio_iter
            else:
                for dosaggio_col in ["Dosaggio Cemento [kg/m³]", "Dosaggio Acqua [l/m³]", "Dosaggio Additivo 1 [l/m³]", "Dosaggio Additivo 2 [l/m³]", "Dosaggio Additivo 3 [l/m³]"]:
                    if selected_row.get(dosaggio_col, 0) != 0:
                        m3_iter /= selected_row.get(dosaggio_col, 0)
                        break

            # Obtener los valores actuales de la fila
            lordo_inerte_e_kg = selected_row.get("Inerte E [kg]", 0)
            lordo_inerte_e_l_kg = selected_row.get("Acqua efficace Inerte E [l]", 0)

            # Verificar la compensación de "Inerte E"
            if "Compensazione Inerte E" in selected_row.index:
                compensazione_inerte_e = selected_row["Compensazione Inerte E"]
                if compensazione_inerte_e == "l + kg" or compensazione_inerte_e == "kg":
                    if lordo_inerte_e_kg != 0:
                        # Calcular el valor neto y el teórico de "Inerte E"
                        inerte_e = lordo_inerte_e_kg - lordo_inerte_e_l_kg
                        umidita_eff_e = lordo_inerte_e_l_kg / inerte_e if inerte_e != 0 else 0
                        teorico_inerte_e = m3_iter * selected_row.get("Dosaggio Inerte E [kg/m³]", 0) * (1 / (1 - umidita_eff_e))
                        flag_for_kg = "kg" if compensazione_inerte_e == "kg" else "l + kg"
                    else:
                        umidita_eff_e = 0
                        teorico_inerte_e = m3_iter * selected_row.get("Dosaggio Inerte E [kg/m³]", 0)
                elif compensazione_inerte_e == "l":
                    teorico_inerte_e = m3_iter * selected_row.get("Dosaggio Inerte E [kg/m³]", 0)
                    flag_for_kg = "l (only water)"
                else:
                    teorico_inerte_e = m3_iter * selected_row.get("Dosaggio Inerte E [kg/m³]", 0)
                    flag_for_kg = "None"
            else:
                teorico_inerte_e = m3_iter * selected_row.get("Dosaggio Inerte E [kg/m³]", 0)
                flag_for_kg = "None"

            # Guardar resultados en listas para la producción actual
            lordo_inerte_e_l_kg_values.append(lordo_inerte_e_l_kg)
            lordo_inerte_e_kg_values.append(lordo_inerte_e_kg)
            lordo_inerte_e_values.append(lordo_inerte_e_kg + lordo_inerte_e_l_kg)
            teorico_inerte_e_values.append(teorico_inerte_e)
            compensazione_valuese.append(flag_for_kg)

            # Calcular variación y porcentaje para la producción actual
            var_inerte_e = teorico_inerte_e - lordo_inerte_e_kg  # Usar el valor bruto para la variación
            if teorico_inerte_e != 0:
                percentage_inerte_e = (var_inerte_e / teorico_inerte_e) * 100
            else:
                percentage_inerte_e = float('nan')

            var_inerte_e_values.append(var_inerte_e)
            percentage_inerte_e_values.append(percentage_inerte_e)

    
        # -------------------------------------------------------------------------------------- CEMENTO
        # -------------------------------------------------------------------------------------- CEMENTO
        # Calcular teorico_cemento para cada fila seleccionada de manera individual
        teorico_cemento_values = []
        for index in selected_indexes:
            # Reinicializar variables locales en cada iteración
            selected_row = filtered_data.iloc[index]
            teorico_cemento = 0

            # Calcular m3 individualmente para cada iteración
            inerte_a_iter = selected_row.get("Inerte A [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A [l]", 0) / selected_row.get("Inerte A [kg]", 0) if selected_row.get("Inerte A [kg]", 0) != 0 else 0))
            inerte_a2_iter = selected_row.get("Inerte A2 [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A2 [l]", 0) / selected_row.get("Inerte A2 [kg]", 0) if selected_row.get("Inerte A2 [kg]", 0) != 0 else 0))
            inerte_b_iter = selected_row.get("Inerte B [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte B [l]", 0) / selected_row.get("Inerte B [kg]", 0) if selected_row.get("Inerte B [kg]", 0) != 0 else 0))
            inerte_c_iter = selected_row.get("Inerte C [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte C [l]", 0) / selected_row.get("Inerte C [kg]", 0) if selected_row.get("Inerte C [kg]", 0) != 0 else 0))
            inerte_d_iter = selected_row.get("Inerte D [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte D [l]", 0) / selected_row.get("Inerte D [kg]", 0) if selected_row.get("Inerte D [kg]", 0) != 0 else 0))
            inerte_e_iter = selected_row.get("Inerte E [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte E [l]", 0) / selected_row.get("Inerte E [kg]", 0) if selected_row.get("Inerte E [kg]", 0) != 0 else 0))

            m3_iter = inerte_a_iter + inerte_a2_iter + inerte_b_iter + inerte_c_iter + inerte_d_iter + inerte_e_iter
            dosaggio_iter = sum([selected_row.get(f"Dosaggio Inerte {inerte} [kg/m³]", 0) for inerte in "A A2 B C D E".split()])
            if dosaggio_iter != 0:
                m3_iter /= dosaggio_iter
            else:
                for dosaggio_col in ["Dosaggio Cemento [kg/m³]", "Dosaggio Acqua [l/m³]", "Dosaggio Additivo 1 [l/m³]", "Dosaggio Additivo 2 [l/m³]", "Dosaggio Additivo 3 [l/m³]"]:
                    if selected_row.get(dosaggio_col, 0) != 0:
                        m3_iter /= selected_row.get(dosaggio_col, 0)
                        break

            # Calcular el valor teórico de "Cemento"
            if selected_row.get("Cemento [kg]", 0) != 0:
                teorico_cemento = m3_iter * selected_row.get("Dosaggio Cemento [kg/m³]", 0)
            else:
                teorico_cemento = 0  # Si "Cemento [kg]" es cero o no existe, establecer en cero

            # Guardar el valor teórico calculado
            teorico_cemento_values.append(teorico_cemento)

        # Calcular var_cemento y porcentaje_cemento para cada fila seleccionada
        var_cemento_values = []
        percentage_cemento_values = []

        for index, teorico_cemento_value in zip(selected_indexes, teorico_cemento_values):
            selected_row = filtered_data.iloc[index]
            var_cemento = 0
            percentage_cemento = float('nan')

            # Calcular variación y porcentaje
            lordo_cemento_value = selected_row.get("Cemento [kg]", 0)
            var_cemento = round(teorico_cemento_value - lordo_cemento_value, 2)  # Variación
            var_cemento_values.append(var_cemento)

            # Calcular el porcentaje de variación solo si el valor teórico no es cero
            if teorico_cemento_value != 0:
                percentage_cemento = round((var_cemento / teorico_cemento_value) * 100, 2)
            percentage_cemento_values.append(percentage_cemento)

        # -------------------------------------------------------------------------------------- FILLER
        # -------------------------------------------------------------------------------------- FILLER
        # Calcular teorico_filler para cada fila seleccionada de manera individual
        teorico_filler_values = []
        for index in selected_indexes:
            # Reinicializar variables locales en cada iteración
            selected_row = filtered_data.iloc[index]
            teorico_filler = 0

            # Calcular m3 individualmente para cada iteración
            inerte_a_iter = selected_row.get("Inerte A [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A [l]", 0) / selected_row.get("Inerte A [kg]", 0) if selected_row.get("Inerte A [kg]", 0) != 0 else 0))
            inerte_a2_iter = selected_row.get("Inerte A2 [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A2 [l]", 0) / selected_row.get("Inerte A2 [kg]", 0) if selected_row.get("Inerte A2 [kg]", 0) != 0 else 0))
            inerte_b_iter = selected_row.get("Inerte B [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte B [l]", 0) / selected_row.get("Inerte B [kg]", 0) if selected_row.get("Inerte B [kg]", 0) != 0 else 0))
            inerte_c_iter = selected_row.get("Inerte C [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte C [l]", 0) / selected_row.get("Inerte C [kg]", 0) if selected_row.get("Inerte C [kg]", 0) != 0 else 0))
            inerte_d_iter = selected_row.get("Inerte D [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte D [l]", 0) / selected_row.get("Inerte D [kg]", 0) if selected_row.get("Inerte D [kg]", 0) != 0 else 0))
            inerte_e_iter = selected_row.get("Inerte E [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte E [l]", 0) / selected_row.get("Inerte E [kg]", 0) if selected_row.get("Inerte E [kg]", 0) != 0 else 0))

            m3_iter = inerte_a_iter + inerte_a2_iter + inerte_b_iter + inerte_c_iter + inerte_d_iter + inerte_e_iter
            dosaggio_iter = sum([selected_row.get(f"Dosaggio Inerte {inerte} [kg/m³]", 0) for inerte in "A A2 B C D E".split()])
            if dosaggio_iter != 0:
                m3_iter /= dosaggio_iter
            else:
                for dosaggio_col in ["Dosaggio Cemento [kg/m³]", "Dosaggio Acqua [l/m³]", "Dosaggio Additivo 1 [l/m³]", "Dosaggio Additivo 2 [l/m³]", "Dosaggio Additivo 3 [l/m³]"]:
                    if selected_row.get(dosaggio_col, 0) != 0:
                        m3_iter /= selected_row.get(dosaggio_col, 0)
                        break

            # Calcular el valor teórico de "Filler"
            if selected_row.get("Filler [kg]", 0) != 0:
                teorico_filler = m3_iter * selected_row.get("Dosaggio Filler [kg/m³]", 0)
            else:
                teorico_filler = 0  # Si "Filler [kg]" es cero o no existe, establecer en cero

            # Guardar el valor teórico calculado
            teorico_filler_values.append(teorico_filler)

        # Calcular var_filler y porcentaje_filler para cada fila seleccionada
        var_filler_values = []
        percentage_filler_values = []

        for index, teorico_filler_value in zip(selected_indexes, teorico_filler_values):
            selected_row = filtered_data.iloc[index]
            var_filler = 0
            percentage_filler = float('nan')

            # Calcular variación y porcentaje
            lordo_filler_value = selected_row.get("Filler [kg]", 0)
            var_filler = round(teorico_filler_value - lordo_filler_value, 2)  # Variación
            var_filler_values.append(var_filler)

            # Calcular el porcentaje de variación solo si el valor teórico no es cero
            if teorico_filler_value != 0:
                percentage_filler = round((var_filler / teorico_filler_value) * 100, 2)
            percentage_filler_values.append(percentage_filler)


        # -------------------------------------------------------------------------------------- ACQUA
        # -------------------------------------------------------------------------------------- ACQUA
        # Inicializar la lista de valores teóricos de agua
        teorico_acqua_values = []
        compensazione_inerte_columns = ["Compensazione Inerte A", "Compensazione Inerte B", "Compensazione Inerte C", "Compensazione Inerte D", "Compensazione Inerte E"]

        for index in selected_indexes:
            # Reinicializar variables locales en cada iteración
            selected_row = filtered_data.iloc[index]
            teorico_acqua = 0

            # Calcular m3 de manera individual para cada producción
            inerte_a_iter = selected_row.get("Inerte A [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A [l]", 0) / selected_row.get("Inerte A [kg]", 0) if selected_row.get("Inerte A [kg]", 0) != 0 else 0))
            inerte_a2_iter = selected_row.get("Inerte A2 [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A2 [l]", 0) / selected_row.get("Inerte A2 [kg]", 0) if selected_row.get("Inerte A2 [kg]", 0) != 0 else 0))
            inerte_b_iter = selected_row.get("Inerte B [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte B [l]", 0) / selected_row.get("Inerte B [kg]", 0) if selected_row.get("Inerte B [kg]", 0) != 0 else 0))
            inerte_c_iter = selected_row.get("Inerte C [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte C [l]", 0) / selected_row.get("Inerte C [kg]", 0) if selected_row.get("Inerte C [kg]", 0) != 0 else 0))
            inerte_d_iter = selected_row.get("Inerte D [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte D [l]", 0) / selected_row.get("Inerte D [kg]", 0) if selected_row.get("Inerte D [kg]", 0) != 0 else 0))
            inerte_e_iter = selected_row.get("Inerte E [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte E [l]", 0) / selected_row.get("Inerte E [kg]", 0) if selected_row.get("Inerte E [kg]", 0) != 0 else 0))

            m3_iter = inerte_a_iter + inerte_a2_iter + inerte_b_iter + inerte_c_iter + inerte_d_iter + inerte_e_iter
            dosaggio_iter = sum([selected_row.get(f"Dosaggio Inerte {inerte} [kg/m³]", 0) for inerte in "A A2 B C D E".split()])
            if dosaggio_iter != 0:
                m3_iter /= dosaggio_iter
            else:
                for dosaggio_col in ["Dosaggio Cemento [kg/m³]", "Dosaggio Acqua [l/m³]", "Dosaggio Additivo 1 [l/m³]", "Dosaggio Additivo 2 [l/m³]", "Dosaggio Additivo 3 [l/m³]"]:
                    if selected_row.get(dosaggio_col, 0) != 0:
                        m3_iter /= selected_row.get(dosaggio_col, 0)
                        break

            # Calcular el valor teórico de agua
            if selected_row.get("Totale correzione acqua [l]", 0) != 0:
                corr_acqua = selected_row["Totale correzione acqua [l]"]
                teorico_acqua = m3_iter * selected_row.get("Dosaggio Acqua [l/m³]", 0) + corr_acqua
            else:
                teorico_acqua = m3_iter * selected_row.get("Dosaggio Acqua [l/m³]", 0)

            # Verificar si alguna columna de compensación indica el uso de agua efectiva
            use_acqua_efficace = any(
                "l" in str(selected_row.get(column, "")) or "l + kg" in str(selected_row.get(column, ""))
                for column in compensazione_inerte_columns if column in selected_row.index
            )

            # Ajustar teorico_acqua si se indica el uso de agua efectiva
            for column in compensazione_inerte_columns:
                if column in selected_row.index:
                    if use_acqua_efficace and ("l" in str(selected_row.get(column, "")) or "l + kg" in str(selected_row.get(column, ""))):
                        teorico_acqua -= selected_row.get(f"Acqua efficace Inerte {column[-1]} [l]", 0)

            # Agregar el valor teórico calculado a la lista
            teorico_acqua_values.append(teorico_acqua)

        # Calcular var_acqua y percentage_acqua para cada fila seleccionada
        var_acqua_values = []
        percentage_acqua_values = []

        for index, teorico_acqua_value in zip(selected_indexes, teorico_acqua_values):
            selected_row = filtered_data.iloc[index]
            var_acqua = 0
            percentage_acqua = float('nan')

            # Obtener el valor real de "Acqua [l]"
            lordo_acqua_value = selected_row.get("Acqua [l]", 0)
            var_acqua = round(teorico_acqua_value - lordo_acqua_value, 3)
            var_acqua_values.append(var_acqua)

            # Calcular el porcentaje de variación solo si el valor teórico no es cero
            if teorico_acqua_value != 0:
                percentage_acqua = round((var_acqua / teorico_acqua_value) * 100, 3)
            percentage_acqua_values.append(percentage_acqua)

        
        # -------------------------------------------------------------------------------------- ADDITIVO 1
        # -------------------------------------------------------------------------------------- ADDITIVO 1
        teorico_additivo1_values = []

        for index in selected_indexes:
            # Reinicializar las variables locales al inicio de cada iteración
            selected_row = filtered_data.iloc[index]
            teorico_additivo1 = 0
            # Calcular m3 de manera individual para cada producción
            inerte_a_iter = selected_row.get("Inerte A [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A [l]", 0) / selected_row.get("Inerte A [kg]", 0) if selected_row.get("Inerte A [kg]", 0) != 0 else 0))
            inerte_a2_iter = selected_row.get("Inerte A2 [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A2 [l]", 0) / selected_row.get("Inerte A2 [kg]", 0) if selected_row.get("Inerte A2 [kg]", 0) != 0 else 0))
            inerte_b_iter = selected_row.get("Inerte B [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte B [l]", 0) / selected_row.get("Inerte B [kg]", 0) if selected_row.get("Inerte B [kg]", 0) != 0 else 0))
            inerte_c_iter = selected_row.get("Inerte C [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte C [l]", 0) / selected_row.get("Inerte C [kg]", 0) if selected_row.get("Inerte C [kg]", 0) != 0 else 0))
            inerte_d_iter = selected_row.get("Inerte D [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte D [l]", 0) / selected_row.get("Inerte D [kg]", 0) if selected_row.get("Inerte D [kg]", 0) != 0 else 0))
            inerte_e_iter = selected_row.get("Inerte E [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte E [l]", 0) / selected_row.get("Inerte E [kg]", 0) if selected_row.get("Inerte E [kg]", 0) != 0 else 0))

            m3_iter = inerte_a_iter + inerte_a2_iter + inerte_b_iter + inerte_c_iter + inerte_d_iter + inerte_e_iter
            dosaggio_iter = sum([selected_row.get(f"Dosaggio Inerte {inerte} [kg/m³]", 0) for inerte in "A A2 B C D E".split()])
            if dosaggio_iter != 0:
                m3_iter /= dosaggio_iter
            else:
                for dosaggio_col in ["Dosaggio Cemento [kg/m³]", "Dosaggio Acqua [l/m³]", "Dosaggio Additivo 1 [l/m³]", "Dosaggio Additivo 2 [l/m³]", "Dosaggio Additivo 3 [l/m³]"]:
                    if selected_row.get(dosaggio_col, 0) != 0:
                        m3_iter /= selected_row.get(dosaggio_col, 0)
                        break
            # Calcular el valor teórico de Additivo 1
            teorico_additivo1 = m3_iter * selected_row.get("Dosaggio Additivo 1 [l/m³]", 0)
            teorico_additivo1_values.append(teorico_additivo1)

        var_additivo1_values = []
        percentage_additivo1_values = []

        for index, teorico_additivo1_value in zip(selected_indexes, teorico_additivo1_values):
            selected_row = filtered_data.iloc[index]
            lordo_additivo1_value = selected_row.get("Additivo 1 [l]", 0)

            # Calcular la variación y el porcentaje de Additivo 1
            var_additivo1 = round(teorico_additivo1_value - lordo_additivo1_value, 2)
            var_additivo1_values.append(var_additivo1)

            percentage_additivo1 = round((var_additivo1 / teorico_additivo1_value) * 100, 2) if teorico_additivo1_value != 0 else float('nan')
            percentage_additivo1_values.append(percentage_additivo1)

        # -------------------------------------------------------------------------------------- ADDITIVO 2
        teorico_additivo2_values = []

        for index in selected_indexes:
            selected_row = filtered_data.iloc[index]
            teorico_additivo2 = 0
            # Calcular m3 de manera individual para cada producción
            inerte_a_iter = selected_row.get("Inerte A [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A [l]", 0) / selected_row.get("Inerte A [kg]", 0) if selected_row.get("Inerte A [kg]", 0) != 0 else 0))
            inerte_a2_iter = selected_row.get("Inerte A2 [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A2 [l]", 0) / selected_row.get("Inerte A2 [kg]", 0) if selected_row.get("Inerte A2 [kg]", 0) != 0 else 0))
            inerte_b_iter = selected_row.get("Inerte B [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte B [l]", 0) / selected_row.get("Inerte B [kg]", 0) if selected_row.get("Inerte B [kg]", 0) != 0 else 0))
            inerte_c_iter = selected_row.get("Inerte C [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte C [l]", 0) / selected_row.get("Inerte C [kg]", 0) if selected_row.get("Inerte C [kg]", 0) != 0 else 0))
            inerte_d_iter = selected_row.get("Inerte D [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte D [l]", 0) / selected_row.get("Inerte D [kg]", 0) if selected_row.get("Inerte D [kg]", 0) != 0 else 0))
            inerte_e_iter = selected_row.get("Inerte E [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte E [l]", 0) / selected_row.get("Inerte E [kg]", 0) if selected_row.get("Inerte E [kg]", 0) != 0 else 0))

            m3_iter = inerte_a_iter + inerte_a2_iter + inerte_b_iter + inerte_c_iter + inerte_d_iter + inerte_e_iter
            dosaggio_iter = sum([selected_row.get(f"Dosaggio Inerte {inerte} [kg/m³]", 0) for inerte in "A A2 B C D E".split()])
            if dosaggio_iter != 0:
                m3_iter /= dosaggio_iter
            else:
                for dosaggio_col in ["Dosaggio Cemento [kg/m³]", "Dosaggio Acqua [l/m³]", "Dosaggio Additivo 1 [l/m³]", "Dosaggio Additivo 2 [l/m³]", "Dosaggio Additivo 3 [l/m³]"]:
                    if selected_row.get(dosaggio_col, 0) != 0:
                        m3_iter /= selected_row.get(dosaggio_col, 0)
                        break

            # Calcular el valor teórico de Additivo 2
            teorico_additivo2 = m3_iter * selected_row.get("Dosaggio Additivo 2 [l/m³]", 0)
            teorico_additivo2_values.append(teorico_additivo2)

        var_additivo2_values = []
        percentage_additivo2_values = []

        for index, teorico_additivo2_value in zip(selected_indexes, teorico_additivo2_values):
            selected_row = filtered_data.iloc[index]
            lordo_additivo2_value = selected_row.get("Additivo 2 [l]", 0)

            # Calcular la variación y el porcentaje de Additivo 2
            var_additivo2 = round(teorico_additivo2_value - lordo_additivo2_value, 2)
            var_additivo2_values.append(var_additivo2)

            percentage_additivo2 = round((var_additivo2 / teorico_additivo2_value) * 100, 2) if teorico_additivo2_value != 0 else float('nan')
            percentage_additivo2_values.append(percentage_additivo2)

        # -------------------------------------------------------------------------------------- ADDITIVO 3
        teorico_additivo3_values = []

        for index in selected_indexes:
            selected_row = filtered_data.iloc[index]
            teorico_additivo3 = 0
            # Calcular m3 de manera individual para cada producción
            inerte_a_iter = selected_row.get("Inerte A [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A [l]", 0) / selected_row.get("Inerte A [kg]", 0) if selected_row.get("Inerte A [kg]", 0) != 0 else 0))
            inerte_a2_iter = selected_row.get("Inerte A2 [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte A2 [l]", 0) / selected_row.get("Inerte A2 [kg]", 0) if selected_row.get("Inerte A2 [kg]", 0) != 0 else 0))
            inerte_b_iter = selected_row.get("Inerte B [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte B [l]", 0) / selected_row.get("Inerte B [kg]", 0) if selected_row.get("Inerte B [kg]", 0) != 0 else 0))
            inerte_c_iter = selected_row.get("Inerte C [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte C [l]", 0) / selected_row.get("Inerte C [kg]", 0) if selected_row.get("Inerte C [kg]", 0) != 0 else 0))
            inerte_d_iter = selected_row.get("Inerte D [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte D [l]", 0) / selected_row.get("Inerte D [kg]", 0) if selected_row.get("Inerte D [kg]", 0) != 0 else 0))
            inerte_e_iter = selected_row.get("Inerte E [kg]", 0) * (1 - (selected_row.get("Acqua efficace Inerte E [l]", 0) / selected_row.get("Inerte E [kg]", 0) if selected_row.get("Inerte E [kg]", 0) != 0 else 0))

            m3_iter = inerte_a_iter + inerte_a2_iter + inerte_b_iter + inerte_c_iter + inerte_d_iter + inerte_e_iter
            dosaggio_iter = sum([selected_row.get(f"Dosaggio Inerte {inerte} [kg/m³]", 0) for inerte in "A A2 B C D E".split()])
            if dosaggio_iter != 0:
                m3_iter /= dosaggio_iter
            else:
                for dosaggio_col in ["Dosaggio Cemento [kg/m³]", "Dosaggio Acqua [l/m³]", "Dosaggio Additivo 1 [l/m³]", "Dosaggio Additivo 2 [l/m³]", "Dosaggio Additivo 3 [l/m³]"]:
                    if selected_row.get(dosaggio_col, 0) != 0:
                        m3_iter /= selected_row.get(dosaggio_col, 0)
                        break

            # Calcular el valor teórico de Additivo 3
            teorico_additivo3 = m3_iter * selected_row.get("Dosaggio Additivo 3 [l/m³]", 0)
            teorico_additivo3_values.append(teorico_additivo3)

        var_additivo3_values = []
        percentage_additivo3_values = []

        for index, teorico_additivo3_value in zip(selected_indexes, teorico_additivo3_values):
            selected_row = filtered_data.iloc[index]
            lordo_additivo3_value = selected_row.get("Additivo 3 [l]", 0)

            # Calcular la variación y el porcentaje de Additivo 3
            var_additivo3 = round(teorico_additivo3_value - lordo_additivo3_value, 2)
            var_additivo3_values.append(var_additivo3)

            percentage_additivo3 = round((var_additivo3 / teorico_additivo3_value) * 100, 2) if teorico_additivo3_value != 0 else float('nan')
            percentage_additivo3_values.append(percentage_additivo3)



        # Create HTML table with data displayed in columns
        html = "<html><head><title>Selected Data</title>"
        html += """
        <style>
            table {
                font-family: Arial, sans-serif;
                border-collapse: collapse;
                width: 100%;
            }
            th {
                background-color: #f2f2f2;
                border: 1px solid #dddddd;
                text-align: left;
                padding: 8px;
            }
            td {
                border: 1px solid #dddddd;
                text-align: left;
                padding: 8px;
            }
            tr:nth-child(even) {
                background-color: #dddddd;
            }
        </style>
        """
        html += "</head><body><table>"

        # Add headers as the first row, excluding "Unnamed" columns
        headers = [col for col in filtered_data.columns if 'Unnamed' not in col]
        html += "<tr><th></th>" + "".join(f"<th>{filtered_data.iloc[index]['Ora start']}</th>" for index in selected_indexes) + "</tr>"

        # Iterate over each header
        for i, col in enumerate(headers):
            html += f"<tr><td><b>{col}</b></td>"
            # Iterate over each selected index
            for index in selected_indexes:
                selected_row = filtered_data.iloc[index]
                # Add data cells
                html += f"<td>{selected_row[col]}</td>"

            # Add new rows after "Inerte A lordo compensato [kg]"
            if col == "Inerte A [kg]":
                html += "<tr><td><b>Inerte A lordo compensato [kg]</b></td>"
                for value in lordo_inerte_a_values:
                    html += f"<td>{value}</td>"
                html += "</tr>"

                html += "<tr><td><b>Teorico Inerte A [kg]</b></td>"
                for value in teorico_inerte_a_values:
                    html += f"<td>{round(value,2)}</td>"
                html += "</tr>"

                html += "<tr><td><b>Compensazione Inerte A</b></td>"
                for value in compensazione_valuesa:
                    html += f"<td>{value}</td>"
                html += "</tr>"

                html += "<tr><td><b>Var. Inerte A [kg]</b></td>"
                for var_inerte_a, percentage_inerte_a in zip(var_inerte_a_values, percentage_inerte_a_values):
                    # Determine color based on percentage value
                    if pd.isna(percentage_inerte_a):
                        color = "white"
                    elif abs(percentage_inerte_a) <= 3:
                        color = "lightgreen"
                    elif abs(percentage_inerte_a) <= 5:
                        color = "#ffe6b3"  # soft orange
                    else:
                        color = "#ffd9d9"  # soft red
                    html += f"<td style='background-color:{color}'>{var_inerte_a:.2f} ({percentage_inerte_a:.2f} [%])</td>"

                html += "</tr>"

            html += "</tr>"
            # Add new rows after "Inerte A2 lordo compensato [kg]"
            if col == "Inerte A2 [kg]":
                html += "<tr><td><b>Inerte A2 lordo compensato [kg]</b></td>"
                for value in lordo_inerte_a2_values:
                    html += f"<td>{value}</td>"
                html += "</tr>"

                html += "<tr><td><b>Teorico Inerte A2 [kg]</b></td>"
                for value2 in teorico_inerte_a2_values:
                    html += f"<td>{round(value2,2)}</td>"
                html += "</tr>"

                html += "<tr><td><b>Compensazione Inerte A2</b></td>"
                for value2 in compensazione_valuesa2:
                    html += f"<td>{value2}</td>"
                html += "</tr>"

                html += "<tr><td><b>Var. Inerte A [kg]</b></td>"
                for var_inerte_a2, percentage_inerte_a2 in zip(var_inerte_a2_values, percentage_inerte_a2_values):
                    # Determine color based on percentage value
                    if pd.isna(percentage_inerte_a2):
                        color = "white"
                    elif abs(percentage_inerte_a2) <= 3:
                        color = "lightgreen"
                    elif abs(percentage_inerte_a2) <= 5:
                        color = "#ffe6b3"  # soft orange
                    else:
                        color = "#ffd9d9"  # soft red
                    html += f"<td style='background-color:{color}'>{var_inerte_a2:.2f} ({percentage_inerte_a2:.2f} [%])</td>"

                html += "</tr>"

            html += "</tr>"
            # Add new rows after "Inerte B lordo compensato [kg]"
            if col == "Inerte B [kg]":
                html += "<tr><td><b>Inerte B lordo compensato [kg]</b></td>"
                for value, compensazione in zip(lordo_inerte_b_values, compensazione_valuesb):
                    if compensazione in ["kg", "l + kg"]:
                        html += f"<td>{value}</td>"
                html += "</tr>"

                html += "<tr><td><b>Teorico Inerte B [kg]</b></td>"
                for valueb in teorico_inerte_b_values:
                    html += f"<td>{round(valueb, 2)}</td>"
                html += "</tr>"

                html += "<tr><td><b>Compensazione Inerte B</b></td>"
                for valueb in compensazione_valuesb:
                    html += f"<td>{valueb}</td>"
                html += "</tr>"

                html += "<tr><td><b>Var. Inerte B [kg]</b></td>"
                for var_inerte_b, percentage_inerte_b in zip(var_inerte_b_values, percentage_inerte_b_values):
                    # Determine color based on percentage value
                    if pd.isna(percentage_inerte_b):
                        color = "white"
                    elif abs(percentage_inerte_b) <= 3:
                        color = "lightgreen"
                    elif abs(percentage_inerte_b) <= 5:
                        color = "#ffe6b3"  # soft orange
                    else:
                        color = "#ffd9d9"  # soft red
                    html += f"<td style='background-color:{color}'>{var_inerte_b:.2f} ({percentage_inerte_b:.2f} [%])</td>"

                html += "</tr>"

            html += "</tr>"
            # Add new rows after "Inerte C lordo compensato [kg]"
            if col == "Inerte C [kg]":
                html += "<tr><td><b>Inerte C lordo compensato [kg]</b></td>"
                for value, compensazione in zip(lordo_inerte_c_values, compensazione_valuesc):
                    if compensazione in ["kg", "l + kg"]:
                        html += f"<td>{value}</td>"
                html += "</tr>"

                html += "<tr><td><b>Teorico Inerte C [kg]</b></td>"
                for value in teorico_inerte_c_values:
                    html += f"<td>{round(value,2)}</td>"
                html += "</tr>"

                html += "<tr><td><b>Compensazione Inerte C</b></td>"
                for value in compensazione_valuesc:
                    html += f"<td>{value}</td>"
                html += "</tr>"

                html += "<tr><td><b>Var. Inerte C [kg]</b></td>"
                for var_inerte_c, percentage_inerte_c in zip(var_inerte_c_values, percentage_inerte_c_values):
                    # Determine color based on percentage value
                    if pd.isna(percentage_inerte_c):
                        color = "white"
                    elif abs(percentage_inerte_c) <= 3:
                        color = "lightgreen"
                    elif abs(percentage_inerte_c) <= 5:
                        color = "#ffe6b3"  # soft orange
                    else:
                        color = "#ffd9d9"  # soft red
                    html += f"<td style='background-color:{color}'>{var_inerte_c:.2f} ({percentage_inerte_c:.2f} [%])</td>"

                html += "</tr>"

            html += "</tr>"
            # Add new rows after "Inerte D lordo compensato [kg]"
            if col == "Inerte D [kg]":
                html += "<tr><td><b>Inerte D lordo compensato [kg]</b></td>"
                for value, compensazione in zip(lordo_inerte_d_values, compensazione_valuesd):
                    if compensazione in ["kg", "l + kg"]:
                        html += f"<td>{value}</td>"
                html += "</tr>"

                html += "<tr><td><b>Teorico Inerte D [kg]</b></td>"
                for value in teorico_inerte_d_values:
                    html += f"<td>{round(value,2)}</td>"
                html += "</tr>"

                html += "<tr><td><b>Compensazione Inerte D</b></td>"
                for value in compensazione_valuesd:
                    html += f"<td>{value}</td>"
                html += "</tr>"

                html += "<tr><td><b>Var. Inerte D [kg]</b></td>"
                for var_inerte_d, percentage_inerte_d in zip(var_inerte_d_values, percentage_inerte_d_values):
                    # Determine color based on percentage value
                    if pd.isna(percentage_inerte_d):
                        color = "white"
                    elif abs(percentage_inerte_d) <= 3:
                        color = "lightgreen"
                    elif abs(percentage_inerte_d) <= 5:
                        color = "#ffe6b3"  # soft orange
                    else:
                        color = "#ffd9d9"  # soft red
                    html += f"<td style='background-color:{color}'>{var_inerte_d:.2f} ({percentage_inerte_d:.2f} [%])</td>"

                html += "</tr>"

            html += "</tr>"
            # Add new rows after "Inerte E lordo compensato [kg]"
            if col == "Inerte E [kg]":
                html += "<tr><td><b>Inerte E lordo compensato [kg]</b></td>"
                for value, compensazione in zip(lordo_inerte_e_values, compensazione_valuese):
                    if compensazione in ["kg", "l + kg"]:
                        html += f"<td>{value}</td>"
                html += "</tr>"

                html += "<tr><td><b>Teorico Inerte E [kg]</b></td>"
                for value in teorico_inerte_e_values:
                    html += f"<td>{round(value,2)}</td>"
                html += "</tr>"

                html += "<tr><td><b>Compensazione Inerte E</b></td>"
                for value in compensazione_valuese:
                    html += f"<td>{value}</td>"
                html += "</tr>"

                html += "<tr><td><b>Var. Inerte E [kg]</b></td>"
                for var_inerte_e, percentage_inerte_e in zip(var_inerte_e_values, percentage_inerte_e_values):
                    # Determine color based on percentage value
                    if pd.isna(percentage_inerte_e):
                        color = "white"
                    elif abs(percentage_inerte_e) <= 3:
                        color = "lightgreen"
                    elif abs(percentage_inerte_e) <= 5:
                        color = "#ffe6b3"  # soft orange
                    else:
                        color = "#ffd9d9"  # soft red
                    html += f"<td style='background-color:{color}'>{var_inerte_e:.2f} ({percentage_inerte_e:.2f} [%])</td>"

                html += "</tr>"

            html += "</tr>"
            # Add new rows after "Cemento [kg]"
            if col == "Cemento [kg]":
                html += "<tr><td><b>Teorico Cemento [kg]</b></td>"
                for value in teorico_cemento_values:
                    html += f"<td>{round(value,2)}</td>"
                html += "</tr>"

                html += "<tr><td><b>Var. Cemento [kg]</b></td>"
                for var_cemento, percentage_cemento in zip(var_cemento_values, percentage_cemento_values):
                    # Determine color based on percentage value
                    if pd.isna(percentage_cemento):
                        color = "white"
                    elif abs(percentage_cemento) <= 3:
                        color = "lightgreen"
                    elif abs(percentage_cemento) <= 5:
                        color = "#ffe6b3"  # soft orange
                    else:
                        color = "#ffd9d9"  # soft red
                    html += f"<td style='background-color:{color}'>{var_cemento:.2f} ({percentage_cemento:.2f} [%])</td>"

                html += "</tr>"

            html += "</tr>"
            # Add new rows after "Filler [kg]"
            if col == "Filler [kg]":
                html += "<tr><td><b>Teorico Filler [kg]</b></td>"
                for value in teorico_filler_values:
                    html += f"<td>{round(value,2)}</td>"
                html += "</tr>"

                html += "<tr><td><b>Var. Cemento [kg]</b></td>"
                for var_filler, percentage_filler in zip(var_filler_values, percentage_filler_values):
                    # Determine color based on percentage value
                    if pd.isna(percentage_filler):
                        color = "white"
                    elif abs(percentage_filler) <= 3:
                        color = "lightgreen"
                    elif abs(percentage_filler) <= 5:
                        color = "#ffe6b3"  # soft orange
                    else:
                        color = "#ffd9d9"  # soft red
                    html += f"<td style='background-color:{color}'>{var_filler:.2f} ({percentage_filler:.2f} [%])</td>"

                html += "</tr>"

            html += "</tr>"
            # Add new rows after "Acqua [l]"
            if col == "Acqua [l]":
                html += "<tr><td><b>Teorico Acqua [l]</b></td>"
                for value in teorico_acqua_values:
                    html += f"<td>{round(value,2)}</td>"
                html += "</tr>"

                html += "<tr><td><b>Var. Acqua [l]</b></td>"
                for var_acqua, percentage_acqua in zip(var_acqua_values, percentage_acqua_values):
                    # Determine color based on percentage value
                    if pd.isna(percentage_acqua):
                        color = "white"
                    elif abs(percentage_acqua) <= 3:
                        color = "lightgreen"
                    elif abs(percentage_acqua) <= 5:
                        color = "#ffe6b3"  # soft orange
                    else:
                        color = "#ffd9d9"  # soft red
                    html += f"<td style='background-color:{color}'>{var_acqua:.2f} ({percentage_acqua:.2f} [%])</td>"

                html += "</tr>"

            html += "</tr>"
            # Add new rows after "Additivo 1 [l]"
            if col == "Additivo 1 [l]":
                html += "<tr><td><b>Teorico Additivo 1 [l]</b></td>"
                for value in teorico_additivo1_values:
                    html += f"<td>{round(value,2)}</td>"
                html += "</tr>"

                html += "<tr><td><b>Var. Additivo 1 [l]</b></td>"
                for var_additivo1, percentage_additivo1 in zip(var_additivo1_values, percentage_additivo1_values):
                    # Determine color based on percentage value
                    if pd.isna(percentage_additivo1):
                        color = "white"
                    elif abs(percentage_additivo1) <= 3:
                        color = "lightgreen"
                    elif abs(percentage_additivo1) <= 5:
                        color = "#ffe6b3"  # soft orange
                    else:
                        color = "#ffd9d9"  # soft red
                    html += f"<td style='background-color:{color}'>{var_additivo1:.2f} ({percentage_additivo1:.2f} [%])</td>"

                html += "</tr>"

            html += "</tr>"
            # Add new rows after "Additivo 2 [l]"
            if col == "Additivo 2 [l]":
                html += "<tr><td><b>Teorico Additivo 2 [l]</b></td>"
                for value in teorico_additivo2_values:
                    html += f"<td>{round(value,2)}</td>"
                html += "</tr>"

                html += "<tr><td><b>Var. Additivo 2 [l]</b></td>"
                for var_additivo2, percentage_additivo2 in zip(var_additivo2_values, percentage_additivo2_values):
                    # Determine color based on percentage value
                    if pd.isna(percentage_additivo2):
                        color = "white"
                    elif abs(percentage_additivo2) <= 3:
                        color = "lightgreen"
                    elif abs(percentage_additivo2) <= 5:
                        color = "#ffe6b3"  # soft orange
                    else:
                        color = "#ffd9d9"  # soft red
                    html += f"<td style='background-color:{color}'>{var_additivo2:.2f} ({percentage_additivo2:.2f} [%])</td>"

                html += "</tr>"

            html += "</tr>"
            # Add new rows after "Additivo 3 [l]"
            if col == "Additivo 3 [l]":
                html += "<tr><td><b>Teorico Additivo 3 [l]</b></td>"
                for value in teorico_additivo3_values:
                    html += f"<td>{round(value,2)}</td>"
                html += "</tr>"

                html += "<tr><td><b>Var. Additivo 3 [l]</b></td>"
                for var_additivo3, percentage_additivo3 in zip(var_additivo3_values, percentage_additivo3_values):
                    # Determine color based on percentage value
                    if pd.isna(percentage_additivo3):
                        color = "white"
                    elif abs(percentage_additivo3) <= 3:
                        color = "lightgreen"
                    elif abs(percentage_additivo3) <= 5:
                        color = "#ffe6b3"  # soft orange
                    else:
                        color = "#ffd9d9"  # soft red
                    html += f"<td style='background-color:{color}'>{var_additivo3:.2f} ({percentage_additivo3:.2f} [%])</td>"

                html += "</tr>"

            html += "</tr>"
        html += "</table></body></html>"

        # Save HTML table to a file
        html_file_path = generate_and_save_temp_html_table(html)
        html_file_path = generate_and_save_html_table(html)

        if html_file_path:
            html_label = ttk.Label(new_feature_frame, text=html_file_path)
            html_label.grid(row=3, column=1, padx=5, pady=5, sticky="w")
        # Open the saved HTML file in default browser
        open_saved_html_file(html_file_path)

    except Exception as e:
        messagebox.showerror("Error", f"4 An error occurred while opening selected data in browser: {str(e)}")

def generate_and_save_temp_html_table(html_content):
    try:
        # Obtén el directorio temporal
        temp_dir = tempfile.gettempdir()

        # Definir la ruta completa del archivo HTML en el directorio temporal
        html_file_path = os.path.join(temp_dir, 'selected_data.html')
        
        # Escribir el contenido HTML en el archivo HTML en el directorio temporal
        with open(html_file_path, 'w') as f:
            f.write(html_content)

        return temp_dir, html_file_path  # Retorna la ruta del directorio y del archivo HTML guardado

    except Exception as e:
        # Maneja cualquier error que pueda ocurrir
        messagebox.showerror("Error", f"5 An error occurred while saving selected data: {str(e)}")
        return None, None

# Function to generate and save HTML table
def generate_and_save_html_table(html_content):
    try:
        # Obtener la ruta del directorio actual donde se ejecutó el programa
        current_dir = os.getcwd()

        # Definir la ruta completa del archivo HTML en el directorio actual
        html_file_path = os.path.join(current_dir, 'selected_data.html')
        print(html_file_path)
        # Escribir el contenido HTML en el archivo HTML en el directorio actual
        with open(html_file_path, 'w') as f:
            f.write(html_content)

        return html_file_path  # Retorna la ruta del archivo HTML guardado

    except Exception as e:
        # Maneja cualquier error que pueda ocurrir
        messagebox.showerror("Error", f"6 An error occurred while saving selected data: {str(e)}")
        return None
    
# Function to open the saved HTML file in default browser
def open_saved_html_file(html_file_path):
    try:
        if html_file_path:
            webbrowser.open(html_file_path, new=2)
        else:
            messagebox.showerror("Error", "No HTML file saved")

    except Exception as e:
        messagebox.showerror("Error", f"7 An error occurred while opening saved HTML file: {str(e)}")


def update_available_data():
    global available_dates, available_event_types, available_components
    
    # Exclude specific event types
    excluded_event_types = ["Aggr. moisture", "Start cycle", "Stop cycle", "Water corr.","Start pause","Stop pause"]
    
    available_dates = ['All'] + data['Date [dd/mm/yyyy]'].unique().tolist()  # Add the "All" option
    available_event_types = sorted([event_type for event_type in data['Event type'].unique() if event_type not in excluded_event_types])

    available_components = sorted(data.columns[4:].tolist())
    # Exclude 'Microsecond' and 'Time [hh:mm:ss.micro]' from available components
    excluded_components = ['Microsecond', 'Time [hh:mm:ss.micro]','Datetime','Datetime [yyyy-mm-dd hh:mm:ss.micro]']
    available_components = [comp for comp in available_components if comp not in excluded_components]
    
    listbox_dates.delete(0, tk.END)
    for date in available_dates:
        listbox_dates.insert(tk.END, date)
    
    listbox_event_types.delete(0, tk.END)
    for event_type in available_event_types:
        listbox_event_types.insert(tk.END, event_type)




def find_productions(selected_date):
    productions = []
    if selected_date == 'All':
        
        # If "All" is selected, return all available hourly productions across the entire dataset
        hourly_events = data['Time [hh:mm:ss]'].tolist()
        event_types = data['Event type'].tolist()

        # Find the first occurrence of "Start cycle" event
        try:
            start_index = event_types.index('Start cycle')
        except ValueError:
            return []  # If no "Start cycle" event is found, return an empty list

        # Iterate over start indices to find hourly productions
        for i in range(start_index, len(event_types)):
            if event_types[i] == 'Stop cycle':
                production_start = hourly_events[start_index]
                production_end = hourly_events[i]
                # Ensure start time is earlier than end time
                if production_start > production_end:
                    production_start, production_end = production_end, production_start
                productions.append(f'{production_start} - {production_end}')
                # Update start index for the next hourly production
                start_index = i + 1

    else:
        # If a specific date is selected, get the hourly productions available only for that date
        date_events = data[data['Date [dd/mm/yyyy]'] == selected_date]['Event type'].tolist()
        hourly_events = data[data['Date [dd/mm/yyyy]'] == selected_date]['Time [hh:mm:ss]'].tolist()

        # Find the first occurrence of "Start cycle" event
        try:
            start_index = date_events.index('Start cycle')
        except ValueError:
            return []  # If no "Start cycle" event is found, return an empty list

        # Iterate over start indices to find hourly productions
        for i in range(start_index, len(date_events)):
            if date_events[i] == 'Stop cycle':
                production_start = hourly_events[start_index]
                production_end = hourly_events[i]
                # Ensure start time is earlier than end time
                if production_start > production_end:
                    production_start, production_end = production_end, production_start
                productions.append(f'{production_start} - {production_end}')
                # Update start index for the next hourly production
                start_index = i + 1

    return productions

def update_selected_dates(event):
    global selected_dates
    if listbox_dates.curselection():
        selected_dates = [listbox_dates.get(idx) for idx in listbox_dates.curselection()]

        # If "All" is selected, disable the hourly productions listbox
        if 'All' in selected_dates:
            update_listbox_state(listbox_hourly_productions, tk.DISABLED)
            update_listbox_state(listbox_event_types, tk.NORMAL)
        else:
            # Enable the hourly productions listbox
            update_listbox_state(listbox_hourly_productions, tk.NORMAL)
            update_listbox_state(listbox_event_types, tk.DISABLED)
            
            # Mostrar producciones horarias de todas las fechas seleccionadas
            listbox_hourly_productions.delete(0, tk.END)
            for selected_date in selected_dates:
                available_hourly_productions = sorted(find_productions(selected_date))
                for production in available_hourly_productions:
                    # Incluye la fecha en el string para poder identificarla luego
                    listbox_hourly_productions.insert(tk.END, f"{selected_date} | {production}")
            
            # Update available event types based on the selected date
            update_available_data()  # Call the function here to update event types



def update_selected_hourly_productions(event):
    global selected_dates, selected_hourly_productions, selected_event_types
    # Ahora cada item es "fecha | hora"
    selected_hourly_productions = [listbox_hourly_productions.get(idx) for idx in listbox_hourly_productions.curselection()]
    update_listbox_state(listbox_event_types, tk.NORMAL)

def update_selected_event_types(event):
    global selected_event_types, selected_components, data, selected_hourly_productions, selected_dates

    selected_event_types = [listbox_event_types.get(idx) for idx in listbox_event_types.curselection()]
    


    for selected_event_type in selected_event_types:
        if selected_event_type in data['Event type'].unique():
            filtered_data = data[data['Event type'].isin([selected_event_type, 'Start cycle', 'Stop cycle', 'Start pause', 'Stop pause'])].copy()

            selected_date = selected_dates[0]
            if selected_date:
                if selected_date == 'All':
                    messagebox.showwarning("Warning", "By selecting 'All' dates, the coefficients will be calculated for all available production, which can take time and result in non-representative values.")
                    return  # Salir de la función si se selecciona "All" como fecha
                
                if selected_event_type in ['Water', 'Cement', 'Aggregate A', 'Aggregate B', 'Aggregate C', 'Aggregate D', 'Admixture 1', 'Admixture 2']:
                    if selected_hourly_productions and selected_hourly_productions[0] != "All":
                        start_times = [prod.split(' - ')[0] for prod in selected_hourly_productions]
                        end_times = [prod.split(' - ')[1] for prod in selected_hourly_productions]

                        # Convert selected dates to datetime objects
                        selected_date = datetime.strptime(selected_date, '%d/%m/%Y')

                        # Filter data by selected date and hourly productions
                        for start_time, end_time in zip(start_times, end_times):
                            start_datetime = selected_date.replace(hour=int(start_time[:2]), minute=int(start_time[3:5]), second=int(start_time[6:]))
                            end_datetime = selected_date.replace(hour=int(end_time[:2]), minute=int(end_time[3:5]), second=int(end_time[6:]))
                                                        
                            # Convert datetime strings to datetime objects
                            filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'] = pd.to_datetime(filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'])

                            # Filter data by selected date and hourly productions
                            filtered_data = filtered_data[(filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'] >= start_datetime) & 
                                                            (filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'] <= end_datetime)]
                    EXCLUDED_EVENTS = {'Cement', 'Filler', 'Water', 'Admixture 1', 'Admixture 2', 'Admixture 3'}
                    CALIBRATION_THRESHOLD = 0.2  # 20%

                    if not filtered_data.empty:

                        # Verificar selección única en Listboxes
                        if len(selected_hourly_productions) != 1 or len(selected_event_types) != 1:
                            messagebox.showerror("Error", "Select exactly one hourly production range and one event type.")
                        else:
                            # Verificar exclusión de eventos
                            if not set(EXCLUDED_EVENTS).intersection(filtered_data['Event type'].unique()):
                                # Calcular Resa Vena
                                resa_vena = filtered_data['PV'].mean() / filtered_data['Sensor'].mean()
                                resa_vena_text = f"{selected_event_type} Resa Vena: {resa_vena:.2f}"  # Formato con 2 decimales

                                # Eliminar etiqueta existente si ya fue creada
                                if hasattr(telemetry_frame, 'resa_vena_label'):
                                    telemetry_frame.resa_vena_label.destroy()

                                # Crear una nueva etiqueta para Resa Vena
                                telemetry_frame.resa_vena_label = ttk.Label(telemetry_frame, text=resa_vena_text)
                                telemetry_frame.resa_vena_label.grid(row=3, column=0, columnspan=2, padx=5, pady=5, sticky="w")
                            else:
                                # Construir el mensaje para los eventos excluidos
                                excluded_events = ", ".join(
                                    [event for event in EXCLUDED_EVENTS if event in filtered_data['Event type'].unique()]
                                )
                                messagebox.showinfo("Info", f"Resa Vena calculation is not applicable for the following events: {excluded_events}.")

                            # Calcular estado de calibración si las columnas Ev MV y Ev DV están presentes
                            if 'Ev MV' in filtered_data.columns and 'Ev DV' in filtered_data.columns:
                                mean_ev_mv = filtered_data['Ev MV'].mean()
                                mean_ev_dv = filtered_data['Ev DV'].mean()

                                # Comparación para determinar estado de calibración
                                if mean_ev_mv > (1 + CALIBRATION_THRESHOLD) * mean_ev_dv:
                                    calibration_status = "Calibration too low"
                                elif mean_ev_mv < (1 - CALIBRATION_THRESHOLD) * mean_ev_dv:
                                    calibration_status = "Calibration too high"
                                else:
                                    calibration_status = "Calibration within acceptable range"

                                # Construir texto para mostrar en la GUI
                                calibration_text = f"{selected_event_type} Calibration Status: {calibration_status}\n" \
                                                f"Mean Ev MV: {mean_ev_mv:.2f}, Mean Ev DV: {mean_ev_dv:.2f}"

                                # Eliminar etiqueta existente si ya fue creada
                                if hasattr(telemetry_frame, 'calibration_status_label'):
                                    telemetry_frame.calibration_status_label.destroy()

                                # Crear una nueva etiqueta para mostrar el estado de calibración
                                telemetry_frame.calibration_status_label = ttk.Label(
                                    telemetry_frame, text=calibration_text, foreground="darkred"
                                )
                                telemetry_frame.calibration_status_label.grid(row=4, column=0, columnspan=2, padx=5, pady=5, sticky="w")

                    else:
                        pass

                        # messagebox.showerror("Error", f"No data or Resa Vena available for the event {selected_event_type} within the selected date and hourly production. Try a single hourly production for Resa Vena")
                else:
                    messagebox.showerror("Error", f"It is not possible to calculate the coefficients for the event {selected_event_type}.")
            else:
                messagebox.showerror("Error", "Select both a date and an hourly production.")
        else:
            messagebox.showerror("Error", f"The event {selected_event_type} is not available within the given data")



# Function to filter data by date, event type, and hourly production
def filter_data_by_date_and_hourly_production(data, selected_date, selected_event_type, selected_hourly_production):
    try:
        if selected_hourly_production == 'All':
            filtered_data = data[(data['Date [dd/mm/yyyy]'] == selected_date) & (data['Event type'] == selected_event_type)]
        else:
            start_time, end_time = selected_hourly_production.split(' - ')
            start_time = datetime.strptime(start_time, '%H:%M:%S')
            end_time = datetime.strptime(end_time, '%H:%M:%S')

            filtered_data = data[(data['Date [dd/mm/yyyy]'] == selected_date) & 
                                 (data['Event type'] == selected_event_type) & 
                                 (data['Time [hh:mm:ss]'].apply(lambda x: datetime.strptime(x, '%H:%M:%S')) >= start_time) & 
                                 (data['Time [hh:mm:ss]'].apply(lambda x: datetime.strptime(x, '%H:%M:%S')) <= end_time)]

        if filtered_data.empty:
            messagebox.showerror("Error", "No data was found for the selected criteria.")
        else:
            return filtered_data

    except ValueError:
        messagebox.showerror("Error", "The selected hourly production is invalid.")
    
    return pd.DataFrame()


def add_microseconds(data):
    data['Microsecond'] = '000000'  # Initialize microsecond column as string
    unique_event_types = data['Event type'].unique()

    for event_type in unique_event_types:
        event_type_data = data[data['Event type'] == event_type]
        unique_datetimes = event_type_data['Datetime'].unique()

        for datetime in unique_datetimes:
            same_second_samples = event_type_data[event_type_data['Datetime'] == datetime]
            if len(same_second_samples) > 1:
                # Calculate microseconds step
                microsecond_step = 1e6 / len(same_second_samples)
                # Assign microsecond values
                microsecond = 0
                for index, row in same_second_samples.iterrows():
                    data.at[index, 'Microsecond'] = str(int(microsecond)).zfill(6)
                    microsecond += microsecond_step
            else:
                # If there's only one sample for the timestamp, assign a default microsecond value
                data.loc[same_second_samples.index, 'Microsecond'] = '000000'

    # Combine 'Datetime' and 'Microsecond' columns into 'Datetime [yyyy-mm-dd hh:mm:ss.micro]'
    data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'] = data['Datetime'].astype(str) + '.' + data['Microsecond']
    
    # Truncate microseconds to the first microsecond after the decimal point
    data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'] = data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'].apply(lambda x: x.split('.')[0] + '.' + x.split('.')[1][:6])

    return data


def plot_start_stop_lines(filtered_data, fig_subplots):
    if fig_subplots:
        for i, row in filtered_data.iterrows():
            if row['Event type'] == 'Start cycle':
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime']], y=[0], mode='markers', marker=dict(color='green', symbol='circle-open', size=10), name='Start cycle', showlegend=False), row=1, col=1)
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime']], y=[0], mode='markers', marker=dict(color='green', symbol='circle-open', size=10), name='Start cycle', showlegend=False), row=2, col=1)
            elif row['Event type'] == 'Stop cycle':
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime']], y=[0], mode='markers', marker=dict(color='red', symbol='circle-open', size=10), name='Stop cycle', showlegend=False), row=1, col=1)
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime']], y=[0], mode='markers', marker=dict(color='red', symbol='circle-open', size=10), name='Stop cycle', showlegend=False), row=2, col=1)
            elif row['Event type'] == 'Start pause':
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime']], y=[0], mode='markers', marker=dict(color='blue', symbol='circle-open', size=10), name='Start pause', showlegend=False), row=1, col=1)
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime']], y=[0], mode='markers', marker=dict(color='blue', symbol='circle-open', size=10), name='Start pause', showlegend=False), row=2, col=1)
            elif row['Event type'] == 'Stop pause':
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime']], y=[0], mode='markers', marker=dict(color='orange', symbol='circle-open', size=10), name='Stop pause', showlegend=False), row=1, col=1)
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime']], y=[0], mode='markers', marker=dict(color='orange', symbol='circle-open', size=10), name='Stop pause', showlegend=False), row=2, col=1)

def plot_start_stop_lines_individual(filtered_data, fig):
    if fig:
        for i, row in filtered_data.iterrows():
            if row['Event type'] == 'Start cycle':
                fig.add_trace(go.Scatter(x=[row['Datetime']], y=[0], mode='markers', marker=dict(color='green', symbol='circle-open', size=10), name='Start cycle', showlegend=False))
            elif row['Event type'] == 'Stop cycle':
                fig.add_trace(go.Scatter(x=[row['Datetime']], y=[0], mode='markers', marker=dict(color='red', symbol='circle-open', size=10), name='Stop cycle', showlegend=False))
            elif row['Event type'] == 'Start pause':
                fig.add_trace(go.Scatter(x=[row['Datetime']], y=[0], mode='markers', marker=dict(color='blue', symbol='circle-open', size=10), name='Start pause', showlegend=False))
            elif row['Event type'] == 'Stop pause':
                fig.add_trace(go.Scatter(x=[row['Datetime']], y=[0], mode='markers', marker=dict(color='orange', symbol='circle-open', size=10), name='Stop pause', showlegend=False))


def plot_start_stop_lines_without_time_gaps(filtered_data, fig_subplots):
    if fig_subplots:
        for i, row in filtered_data.iterrows():
            if row['Event type'] == 'Start cycle':
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime [yyyy-mm-dd hh:mm:ss.micro]']], y=[0], mode='markers', marker=dict(color='green', symbol='circle-open', size=10), name='Start cycle', showlegend=False), row=1, col=1)
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime [yyyy-mm-dd hh:mm:ss.micro]']], y=[0], mode='markers', marker=dict(color='green', symbol='circle-open', size=10), name='Start cycle', showlegend=False), row=2, col=1)
            elif row['Event type'] == 'Stop cycle':
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime [yyyy-mm-dd hh:mm:ss.micro]']], y=[0], mode='markers', marker=dict(color='red', symbol='circle-open', size=10), name='Stop cycle', showlegend=False), row=1, col=1)
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime [yyyy-mm-dd hh:mm:ss.micro]']], y=[0], mode='markers', marker=dict(color='red', symbol='circle-open', size=10), name='Stop cycle', showlegend=False), row=2, col=1)
            elif row['Event type'] == 'Start pause':
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime [yyyy-mm-dd hh:mm:ss.micro]']], y=[0], mode='markers', marker=dict(color='blue', symbol='circle-open', size=10), name='Start pause', showlegend=False), row=1, col=1)
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime [yyyy-mm-dd hh:mm:ss.micro]']], y=[0], mode='markers', marker=dict(color='blue', symbol='circle-open', size=10), name='Start pause', showlegend=False), row=2, col=1)
            elif row['Event type'] == 'Stop pause':
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime [yyyy-mm-dd hh:mm:ss.micro]']], y=[0], mode='markers', marker=dict(color='orange', symbol='circle-open', size=10), name='Stop pause', showlegend=False), row=1, col=1)
                fig_subplots.add_trace(go.Scatter(x=[row['Datetime [yyyy-mm-dd hh:mm:ss.micro]']], y=[0], mode='markers', marker=dict(color='orange', symbol='circle-open', size=10), name='Stop pause', showlegend=False), row=2, col=1)

def plot_start_stop_lines_individual_without_time_gaps(filtered_data, fig):
    if fig:
        for i, row in filtered_data.iterrows():
            if row['Event type'] == 'Start cycle':
                fig.add_trace(go.Scatter(x=[row['Datetime [yyyy-mm-dd hh:mm:ss.micro]']], y=[0], mode='markers', marker=dict(color='green', symbol='circle-open', size=10), name='Start cycle', showlegend=False))
            elif row['Event type'] == 'Stop cycle':
                fig.add_trace(go.Scatter(x=[row['Datetime [yyyy-mm-dd hh:mm:ss.micro]']], y=[0], mode='markers', marker=dict(color='red', symbol='circle-open', size=10), name='Stop cycle', showlegend=False))
            elif row['Event type'] == 'Start pause':
                fig.add_trace(go.Scatter(x=[row['Datetime [yyyy-mm-dd hh:mm:ss.micro]']], y=[0], mode='markers', marker=dict(color='blue', symbol='circle-open', size=10), name='Start pause', showlegend=False))
            elif row['Event type'] == 'Stop pause':
                fig.add_trace(go.Scatter(x=[row['Datetime [yyyy-mm-dd hh:mm:ss.micro]']], y=[0], mode='markers', marker=dict(color='orange', symbol='circle-open', size=10), name='Stop pause', showlegend=False))

def calculate_hover_info(filtered_data, bb_data):
    hover_text = []
    for val, dt in zip(filtered_data['Sensor'], filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]']):
        matching_row = bb_data[bb_data['Ora'] == dt]
        if not matching_row.empty:
            codice = matching_row['Codice'].iloc[0]
            descrizione = matching_row['Descrizione'].iloc[0]
            hover_text.append(f"Codice: {codice}, Descrizione: {descrizione}")
        else:
            hover_text.append("")
    return hover_text

def plot_quick_plots():
    global available_event_types, selected_event_types, data, selected_hourly_productions, selected_dates, bb_eve_data, bb_alr_data

    if not selected_event_types:
        up_to = available_event_types
    else:
        up_to = selected_event_types

    for selected_event_type in up_to:
        if selected_event_type in data['Event type'].unique():
            # Filter data for the selected event type and other relevant event types
            filtered_data = data[data['Event type'].isin([selected_event_type, 'Start cycle', 'Stop cycle', 'Start pause', 'Stop pause'])].copy()


            # Handle date and time filtering
            if selected_dates and len(selected_dates) == 1 and "All" not in selected_dates:
                selected_date = selected_dates[0]
                if selected_hourly_productions and selected_hourly_productions[0] != "All":
                    start_time = selected_hourly_productions[0].split(' - ')[0] 
                    end_time = selected_hourly_productions[-1].split(' - ')[-1]  

                    start_datetime = datetime.strptime(f"{selected_date} {start_time}", '%d/%m/%Y %H:%M:%S')
                    end_datetime = datetime.strptime(f"{selected_date} {end_time}", '%d/%m/%Y %H:%M:%S')

                    filtered_data = filtered_data[(filtered_data['Datetime'] >= start_datetime) & 
                                                  (filtered_data['Datetime'] <= end_datetime)]
                else:
                    filtered_data = filtered_data[filtered_data['Date [dd/mm/yyyy]'] == selected_date]

            if not filtered_data.empty:
                # Ensure data is correctly ordered and without duplicates
                filtered_data = drop_duplicates_and_keep_stop_cycle_2(filtered_data)
                #filtered_data = filtered_data.sort_values(by='Datetime')

                if selected_event_type == "Oil Pressure" and 'Sensor' in filtered_data.columns:
                    fig = go.Figure()
                    if not bb_eve_data.empty or not bb_alr_data.empty:
                        bb_data = bb_eve_data if not bb_eve_data.empty else bb_alr_data
                        hover_text = calculate_hover_info(filtered_data, bb_data)
                        fig.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['Sensor'], mode='lines', name='Sensor', hovertext=hover_text))
                        for dt, text, val in zip(filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], hover_text, filtered_data['Sensor']):
                            if text:
                                fig.add_trace(go.Scatter(x=[dt], y=[val], mode='markers', marker=dict(color='orange', symbol='star', size=8), hoverinfo='text', text=text, showlegend=False))
                    else:
                        fig.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['Sensor'], mode='lines', name='Sensor'))

                    fig.update_layout(
                        title={'text': f'<b>{selected_event_type}<b>', 'font': {'size': 24, 'family': 'Arial', 'color': 'black'}},
                        xaxis_title='Date and Time',
                        yaxis_title='Ev MV & Ev DV',
                        xaxis=dict(tickformat='%d/%m/%Y %H:%M:%S'),
                        showlegend=True
                    )
                    plot_start_stop_lines_individual_without_time_gaps(filtered_data, fig)

                    fig.show()
                else:
                    fig_subplots = make_subplots(
                        rows=2,
                        cols=1,
                        subplot_titles=(f"Ev MV and Ev DV of {selected_event_type}", f"PV, SV and Sensor of {selected_event_type}"),
                        specs=[[{"secondary_y": False}], [{"secondary_y": True}]]
                    )

                    fig_subplots.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['SV'], mode='lines', name='SV'), row=2, col=1, secondary_y=False)

                    if 'Sensor' in filtered_data.columns:
                        fig_subplots.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['Sensor'], mode='lines', name='Sensor'), row=2, col=1, secondary_y=True)

                    fig_subplots.update_yaxes(title_text="<b>Ev MV & Ev DV</b>", secondary_y=False, row=1, col=1)
                    fig_subplots.update_yaxes(title_text="<b>PV & SV</b>", secondary_y=False, row=2, col=1)
                    fig_subplots.update_yaxes(title_text="<b>Sensor</b>", secondary_y=True, row=2, col=1)

                    fig_subplots.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['Ev DV'], mode='lines', name='Ev DV'), row=1, col=1, secondary_y=False)
                    
                    if not bb_eve_data.empty or not bb_alr_data.empty:
                        bb_data = bb_eve_data if not bb_eve_data.empty else bb_alr_data
                        hover_text = calculate_hover_info(filtered_data, bb_data)
                        fig_subplots.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['PV'], mode='lines', name='PV', hovertext=hover_text), row=2, col=1, secondary_y=False)
                        fig_subplots.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['Ev MV'], mode='lines', name='Ev MV', hovertext=hover_text), row=1, col=1, secondary_y=False)

                        for dt, text_pv, text_ev, val_pv, val_ev in zip(filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], hover_text, hover_text, filtered_data['PV'], filtered_data['Ev MV']):
                            if text_pv or text_ev:
                                if text_pv:
                                    fig_subplots.add_trace(go.Scatter(x=[dt], y=[val_pv], mode='markers', marker=dict(color='orange', symbol='star', size=8), hoverinfo='text', text=text_pv, showlegend=False), row=2, col=1)
                                if text_ev:
                                    fig_subplots.add_trace(go.Scatter(x=[dt], y=[val_ev], mode='markers', marker=dict(color='blue', symbol='star', size=8), hoverinfo='text', text=text_ev, showlegend=False), row=1, col=1)
                    else:
                        fig_subplots.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['PV'], mode='lines', name='PV'), row=2, col=1, secondary_y=False)
                        fig_subplots.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['Ev MV'], mode='lines', name='Ev MV'), row=1, col=1, secondary_y=False)
                    
                    dates_str = ', '.join(selected_dates)
                    title = f"Quick Graph - {selected_event_type}<br>Dates: {dates_str}"
                    fig_subplots.update_layout(
                        title={'text': f'<b>{selected_event_type}<b>', 'font': {'size': 24, 'family': 'Arial', 'color': 'black'}},
                        xaxis_title='Date and Time',
                        yaxis_title='Ev DV & Ev MV',
                        xaxis=dict(tickformat='%d/%m/%Y %H:%M:%S'),
                        showlegend=True
                    )

                    if tolerance_entry.get():
                        include_tolerance(tolerance_entry, filtered_data, fig_subplots)

                    plot_start_stop_lines_without_time_gaps(filtered_data, fig_subplots)
                    
                    if calculate_slopes_var.get() and 'SV' in filtered_data.columns:
                        intervals, slopes = calculate_slopes_in_intervals(filtered_data)
                        for i in range(len(intervals)):
                            mid_index = len(intervals[i]) // 2
                            mid_point = intervals[i].iloc[mid_index]
                            if not np.isnan(slopes[i]) and slopes[i] != 0:
                                fig_subplots.add_annotation(text=f"{slopes[i]:.3f} [u/min]", x=mid_point['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=mid_point['SV'], showarrow=True, arrowhead=1, ax=-50, ay=-50, row=2, col=1)
                    
                    fig_subplots.show()
            else:
                messagebox.showerror("Error", f"No data available for the event {selected_event_type}.")
        else:
            messagebox.showerror("Error", f"The event {selected_event_type} is not available within the given data")


def plot_quick_plots_without_time_gaps():
    global available_event_types, data, selected_hourly_productions, selected_dates

    if not selected_event_types:
        up_to = available_event_types
    else:
        up_to = selected_event_types

    for selected_event_type in up_to:
        if selected_event_type in data['Event type'].unique():
            # Include the selected event type and "Start cycle" and "Stop cycle" event types in filtered data
            filtered_data = data[data['Event type'].isin([selected_event_type, 'Start cycle', 'Stop cycle','Start pause','Stop pause'])].copy()
            
            if selected_dates and len(selected_dates) == 1 and "All" not in selected_dates:
                if selected_hourly_productions and selected_hourly_productions[0] != "All":
                    #selected_hourly_productions.sort()
                    start_time = selected_hourly_productions[0].split(' - ')[0]
                    end_time = selected_hourly_productions[-1].split(' - ')[-1]

                    selected_date = selected_dates[0]
                    start_time = f"{selected_date} {start_time}"
                    end_time = f"{selected_date} {end_time}"

                    start_time = datetime.strptime(start_time, '%d/%m/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                    end_time = datetime.strptime(end_time, '%d/%m/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')

                    start_time += '.000000'
                    end_time += '.000000'

                    filtered_data = filtered_data[(filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'] >= start_time) & 
                                                    (filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'] <= end_time)]

                else:
                    # When selected_hourly_productions is "All", plot all data in the selected_dates
                    if selected_dates and len(selected_dates) == 1 and "All" in selected_dates:
                        selected_date = selected_dates[0]
                        filtered_data = filtered_data[filtered_data['Date [dd/mm/yyyy]'] == selected_date]

            if not filtered_data.empty:
                
                if selected_event_type == "Oil Pressure":
                    if 'Sensor' in filtered_data.columns:
                        fig = go.Figure()
                        if not bb_eve_data.empty or not bb_alr_data.empty and not bb_op_data.empty:
                            bb_data = bb_eve_data if not bb_eve_data.empty else bb_alr_data
                            hover_text = calculate_hover_info(filtered_data, bb_data)
                            fig.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['Sensor'], mode='lines', name='Sensor', hovertext=hover_text))
                            for dt, text, val in zip(filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], hover_text, filtered_data['Sensor']):
                                if text:
                                    fig.add_trace(go.Scatter(x=[dt], y=[val], mode='markers', marker=dict(color='orange', symbol='star', size=8), hoverinfo='text', text=text, showlegend=False))

                        else:
                            filtered_data = drop_duplicates_and_keep_stop_cycle(filtered_data)
                            fig.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['Sensor'], mode='lines', name='Sensor'))

                        fig.update_layout(title=f"Quick Graph - {selected_event_type}", title_font=dict(size=12))
                        #To make the dtick (distance between ticks) variable to the amount of data:
                        M = round((len(filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]']))/10)
                    
                        fig.update_xaxes(type='category', dtick=M, tickfont=dict(size=8), rangebreaks=[dict(values=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'])])
                        plot_start_stop_lines_individual_without_time_gaps(filtered_data, fig)

                        fig.show()
                    else:
                        messagebox.showerror("Error", "No 'Sensor' data available for 'Oil Pressure'.")
                else:
                    fig_subplots = make_subplots(
                        rows=2,
                        cols=1,
                        subplot_titles=(f"Ev MV and Ev DV of {selected_event_type}", f"PV, SV and Sensor of {selected_event_type}"),
                        specs=[[{"secondary_y": False}], [{"secondary_y": True}]]
                    )

                    filtered_data = drop_duplicates_and_keep_stop_cycle(filtered_data)

                    fig_subplots.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['SV'], mode='lines', name='SV'), row=2, col=1, secondary_y=False)

                    if 'Sensor' in filtered_data.columns:
                        fig_subplots.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['Sensor'], mode='lines', name='Sensor'), row=2, col=1, secondary_y=True)

                    fig_subplots.update_yaxes(title_text="<b>Ev MV & Ev DV</b>", secondary_y=False, row=1, col=1)
                    fig_subplots.update_yaxes(title_text="<b>PV & SV</b>", secondary_y=False, row=2, col=1)
                    fig_subplots.update_yaxes(title_text="<b>Sensor</b>", secondary_y=True, row=2, col=1)

                    fig_subplots.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['Ev DV'], mode='lines', name='Ev DV'), row=1, col=1, secondary_y=False)
                    
                    if not bb_eve_data.empty or not bb_alr_data.empty and not bb_op_data.empty:
                        bb_data = bb_eve_data if not bb_eve_data.empty else bb_alr_data
                        hover_text = calculate_hover_info(filtered_data, bb_data)

                        fig_subplots.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['PV'], mode='lines', name='PV', hovertext=hover_text), row=2, col=1, secondary_y=False)
                        fig_subplots.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['Ev MV'], mode='lines', name='Ev MV', hovertext=hover_text), row=1, col=1, secondary_y=False)

                        for dt, text_pv, text_ev, val_pv, val_ev in zip(filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], hover_text, hover_text, filtered_data['PV'], filtered_data['Ev MV']):
                            if text_pv or text_ev:
                                if text_pv:
                                    fig_subplots.add_trace(go.Scatter(x=[dt], y=[val_pv], mode='markers', marker=dict(color='orange', symbol='star', size=8), hoverinfo='text', text=text_pv, showlegend=False), row=2, col=1)
                                if text_ev:
                                    fig_subplots.add_trace(go.Scatter(x=[dt], y=[val_ev], mode='markers', marker=dict(color='blue', symbol='star', size=8), hoverinfo='text', text=text_ev, showlegend=False), row=1, col=1)
                    else:
                        fig_subplots.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['PV'], mode='lines', name='PV'), row=2, col=1, secondary_y=False)
                        fig_subplots.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['Ev MV'], mode='lines', name='Ev MV'), row=1, col=1, secondary_y=False)
                    
                    dates_str = ', '.join(selected_dates)
                    title = f"Quick Graph - {selected_event_type}<br>Dates: {dates_str}"
                    fig_subplots.update_layout(title=title, title_font=dict(size=12))
                    #To make the dtick (distance between ticks) variable to the amount of data:
                    L = round((len(filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]']))/10)

                    if tolerance_entry.get():
                        include_tolerance(tolerance_entry,filtered_data,fig_subplots)
                    
                    fig_subplots.update_xaxes(type='category', dtick=L, tickfont=dict(size=8), rangebreaks=[dict(values=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'])])
                    plot_start_stop_lines_without_time_gaps(filtered_data, fig_subplots)
                    
                    # Calculate slopes and add annotations
                    if calculate_slopes_var.get() and 'SV' in filtered_data.columns:
                        intervals, slopes = calculate_slopes_in_intervals(filtered_data)
                        for i in range(len(intervals)):
                            mid_index = len(intervals[i]) // 2
                            mid_point = intervals[i].iloc[mid_index]
                            # Only add annotations if the slope is not "nan" and not equal to zero
                            if not np.isnan(slopes[i]) and slopes[i] != 0:
                                fig_subplots.add_annotation(text=f"{slopes[i]:.3f} [u/min]", x=mid_point['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=mid_point['SV'], showarrow=True, arrowhead=1, ax=-50, ay=-50, row=2, col=1)
                    
                    fig_subplots.show()

            else:
                messagebox.showerror("Error", f"No data available for the event {selected_event_type}.")
        else:
            messagebox.showerror("Error", f"The event {selected_event_type} is not available within the given data")


def drop_duplicates_and_keep_stop_cycle(filtered_data):
    # Reverse the order of the DataFrame
    filtered_data_reversed = filtered_data.iloc[::-1]

    # Drop duplicates and keep the last occurrence in the original order
    filtered_data_no_duplicates = filtered_data_reversed.drop_duplicates(subset=['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], keep='first')

    # Reverse the DataFrame back to the original order
    filtered_data_no_duplicates = filtered_data_no_duplicates.iloc[::-1]

    # Sort the data by 'Record no.' if needed
    filtered_data = filtered_data_no_duplicates.sort_values(by='Record no.')

    return filtered_data


def drop_duplicates_and_keep_stop_cycle_2(filtered_data):
    # Reverse the order of the DataFrame
    filtered_data_reversed = filtered_data.iloc[::-1]

    # Drop duplicates and keep the last occurrence in the original order
    filtered_data_no_duplicates = filtered_data_reversed.drop_duplicates(subset=['Datetime'], keep='first')

    # Reverse the DataFrame back to the original order
    filtered_data_no_duplicates = filtered_data_no_duplicates.iloc[::-1]

    # Sort the data by 'Record no.' if needed
    filtered_data = filtered_data_no_duplicates.sort_values(by='Datetime [yyyy-mm-dd hh:mm:ss.micro]')

    return filtered_data


def calculate_slopes_in_intervals(data):
    intervals = []
    slopes = []

    start_idx = 0
    for i in range(1, len(data)):
        if data.iloc[i]['SV'] == 0 or i == len(data) - 1:
            intervals.append(data.iloc[start_idx:i + 1])
            start_idx = i + 1

    for interval in intervals:
        mid_index = len(interval) // 2
        if mid_index >= 10:  # Ensure there are at least 11 data points for the slope calculation
            current_time = datetime.strptime(interval.iloc[mid_index]['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], '%Y-%m-%d %H:%M:%S.%f')
            previous_time = datetime.strptime(interval.iloc[mid_index - 10]['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], '%Y-%m-%d %H:%M:%S.%f')
            time_diff = current_time - previous_time
            if time_diff.total_seconds() != 0:
                sv_current = float(interval.iloc[mid_index]['SV'])
                sv_previous = float(interval.iloc[mid_index - 10]['SV'])
                slope = (sv_current - sv_previous) / (10 * time_diff.total_seconds())
                slopes.append(slope * 600)  # Convert to units per minute
            else:
                slopes.append(np.nan)
        else:
            slopes.append(np.nan)

    return intervals, slopes

def include_tolerance(tolerance_entry,filtered_data,fig):
        tolerance = float(tolerance_entry.get())

        if tolerance is not None:
            if 'SV' in filtered_data.columns:
                fig.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['SV'] + tolerance, mode='lines', line=dict(color='rgba(255, 0, 0, 0.5)', width=0), name='SV + Tolerance'), row=2, col=1)
                fig.add_trace(go.Scatter(x=filtered_data['Datetime [yyyy-mm-dd hh:mm:ss.micro]'], y=filtered_data['SV'] - tolerance, mode='lines', line=dict(color='rgba(255, 0, 0, 0.5)', width=0), fill='tonexty', fillcolor='rgba(255, 0, 0, 0.2)', name='SV - Tolerance'), row=2, col=1)

def quick_plot():
    use_range_breaks = range_breaks_var.get()
    if use_range_breaks:
        plot_quick_plots_without_time_gaps()
    else:
        plot_quick_plots()



def exit_program():
    root.quit()


# Variable global para controlar el bucle de consulta
stop_query = False
status_thread = None

# Conexión a la base de datos
def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='dashboard.c6uk8wvgxws6.eu-west-1.rds.amazonaws.com',
            user='producer',
            password='TZORd?^_lDIJHA9821SFfjoi',
            database='dashboard'
        )
        return connection
    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Database connection failed: {err}")
        return None

def execute_query(query, params=None):
    connection = connect_to_database()
    if not connection:
        return None

    try:
        cursor = connection.cursor()
        cursor.execute(query, params)
        if query.lower().startswith("select"):
            results = cursor.fetchall()
            cursor.close()
            return results
        else:
            connection.commit()
            cursor.close()
    except mysql.connector.Error as err:
        messagebox.showerror("Error", f"Error at executing the query: {err}")
    finally:
        connection.close()


# Función para verificar el estado de "Status Transmitted" en loop utilizando el machine_id seleccionado
def loop_to_check_status(machine_id):
    global stop_query

    try:
        while not stop_query:
            result = execute_query(
                'SELECT SQL_NO_CACHE machine_id, command_id, status_trasmitted FROM dashboard.commands '
                'WHERE machine_id = %s ORDER BY date DESC LIMIT 1', 
                (machine_id,)
            )

            if result:
                status = result[0][2]
                print(f"State of the machine {machine_id}: {status}")

                # Mostrar un mensaje cuando el estado cambia a '4'
                if status == 4:
                    print(f"The state of the machine {machine_id} has reach the state '4'. Closing the Loop.")
                    get_file_from_database(machine_id)
                    stop_query = True
                    break

            time.sleep(1)  # Ajusta el intervalo según sea necesario

    except Exception as e:
        messagebox.showerror("Error", f"An error happened inside the query loop: {str(e)}")


# Función para obtener el campo 'file' de la tabla 'fileToRx' y guardarlo en un archivo .txt
def get_file_from_database(machine_id):
    result = execute_query(
        'SELECT file FROM dashboard.fileToRx WHERE machine_id = %s ORDER BY dateandtime DESC LIMIT 1',
        (machine_id,)
    )

    if result:
        file_content = result[0][0]
        print(f"Content of the field 'file' for the machine {machine_id}: {file_content}")

        # Guardar el contenido en un archivo .txt con codificación utf-8
        with open("received_file.txt", "w", encoding="utf-8") as file:
            file.write(file_content)
        print("The content of the field 'file' has been saved in the file 'received_file.txt'.")
    else:
        print(f"the fied 'file' cannot be founded for the machine {machine_id}.")


# Función para enviar un comando a la base de datos y verificar el estado
def execute_command(machine_id, command_id, user="Lautaro"):
    global stop_query, status_thread
    stop_query = False

    execute_query(
        'INSERT INTO dashboard.commands (user, machine_id, command_id, args, status_trasmitted) VALUES (%s, %s, %s, "", 1)',
        (user, machine_id, command_id)
    )
    print(f"Command {command_id} sent to the machine {machine_id}")

    status_thread = threading.Thread(target=loop_to_check_status, args=(machine_id,))
    status_thread.start()

# Función para ejecutar la query Param.txt y luego iniciar un loop que verifique el estado
def query_param2_txt():
    selected_machine = tree.focus()
    if not selected_machine:
        messagebox.showerror("Error", "Please, select a machine.")
        return

    machine_data = tree.item(selected_machine)['values']
    machine_id = machine_data[0]
    execute_command(machine_id, 211)

# Función para ejecutar la query Calib.txt y luego iniciar un loop que verifique el estado
def query_calib2_txt():
    selected_machine = tree.focus()
    if not selected_machine:
        messagebox.showerror("Error", "Please, select a machine.")
        return

    machine_data = tree.item(selected_machine)['values']
    if not machine_data or len(machine_data) < 1:
        messagebox.showerror("Error", "Please, select a valid machine.")
        return

    machine_id = machine_data[0]  # Machine ID correcto extraído del primer elemento de machine_data
    execute_command(machine_id, 212)

# Función para ejecutar la query Calib.txt y luego iniciar un loop que verifique el estado
def query_log_txt():
    selected_machine = tree.focus()
    if not selected_machine:
        messagebox.showerror("Error", "Please, select a machine.")
        return

    machine_data = tree.item(selected_machine)['values']
    if not machine_data or len(machine_data) < 1:
        messagebox.showerror("Error", "Please, select a valid machine.")
        return

    machine_id = machine_data[0]  # Machine ID correcto extraído del primer elemento de machine_data
    execute_command(machine_id, 222)

# Función para detener la consulta y el thread
def stop_status_check():
    global stop_query, status_thread
    stop_query = True
    if status_thread and status_thread.is_alive():
        status_thread.join()
    print("The query has been stopped.")

# Función para ejecutar la consulta SQL y cargar las máquinas en el Treeview
def load_machines():
    results = execute_query(
        'SELECT machine_id, state, dateandtime FROM dashboard.machine_connection_state ORDER BY state DESC, dateandtime DESC'
    )

    if results:
        for row in tree.get_children():
            tree.delete(row)

        for row in results:
            tree.insert('', 'end', values=row)


# Create the main window
root = tk.Tk()
root.title("Blending 1.1.70")
root.geometry("800x750")
# root.iconphoto(False, tk.PhotoImage(file='C:/Users/lauta/Documents/Inbot/BLEND/Blend.png'))

# Disable resizing of the window
root.resizable(False, False)

# Notebook widget for tabs
notebook = ttk.Notebook(root)
notebook.pack(fill="both", expand=True)

# Tab 1: Original GUI
tab1 = ttk.Frame(notebook)
notebook.add(tab1, text="Plot Interface")

# Frame for CSV operations
csv_frame = ttk.LabelFrame(tab1, text="CSV Operations")
csv_frame.pack(side=tk.TOP, padx=10, pady=5, fill="x")

# CSV operations layout
csv_frame.columnconfigure(1, weight=1)  # Allows the label to expand
load_button = ttk.Button(csv_frame, text="Load CSV", command=load_data)
load_button.grid(row=0, column=0, padx=5, pady=5, sticky="w")

files_selected_label = ttk.Label(csv_frame, text="No files selected")
files_selected_label.grid(row=0, column=1, padx=5, pady=5, sticky="w")

# Frame for Telemetry Info
telemetry_frame = ttk.LabelFrame(tab1, text="Telemetry Info")
telemetry_frame.pack(side=tk.TOP, padx=10, pady=5, fill="both", expand=True)

# Grid configuration for Telemetry Frame
telemetry_frame.columnconfigure(0, weight=1)
telemetry_frame.columnconfigure(1, weight=1)

# Variable to hold the last software version
last_version_var = tk.StringVar(value="N/A")  # Default value

# Label for the fixed field
last_version_label = ttk.Label(
    telemetry_frame, 
    text="Last software version:",
    font=("Arial", 10, "bold")
)
last_version_label.grid(row=0, column=0, padx=10, pady=5, sticky="w")

# Dynamic field to display the last software version
last_version_value = ttk.Label(
    telemetry_frame,
    textvariable=last_version_var,  # Bind to StringVar
    font=("Arial", 10),
    foreground="blue"
)
last_version_value.grid(row=0, column=1, padx=10, pady=5, sticky="w")

# Placeholder for telemetry data
telemetry_data_label = ttk.Label(
    telemetry_frame,
    text="",  # No default text
    anchor="center",
    justify="center",
    font=("Arial", 10)
)
telemetry_data_label.grid(row=1, column=0, columnspan=2, padx=10, pady=10, sticky="ew")

# Label for instructional text
instruction_label = ttk.Label(
    telemetry_frame,
    text="To run a specific analysis, select just one option from each listbox.",
    anchor="center",
    justify="center",
    font=("Arial", 10, "italic"),
    foreground="gray"
)
instruction_label.grid(row=2, column=0, columnspan=2, padx=10, pady=10, sticky="ew")

# Frame for listboxes
listbox_frame = ttk.LabelFrame(tab1, text="Data Selection")
listbox_frame.pack(side=tk.TOP, padx=10, pady=5, fill="both", expand=True)

# Layout for listboxes
# Adjust layout for compactness
listbox_frame.columnconfigure((0, 1), weight=1)  # Two columns
listbox_frame.rowconfigure(0, weight=1)
listbox_frame.rowconfigure(1, weight=1)

# Listbox for Dates
ttk.Label(listbox_frame, text="Dates", anchor="center").grid(row=0, column=0, padx=5, pady=5)
listbox_dates = tk.Listbox(listbox_frame, selectmode=tk.MULTIPLE, exportselection=0, height=5)
listbox_dates.grid(row=1, column=0, padx=5, pady=5, sticky="nsew")
listbox_dates.bind("<<ListboxSelect>>", update_selected_dates)

# Listbox for Production Time
label_hourly_productions = ttk.Label(listbox_frame, text="Production Time", anchor="center")
label_hourly_productions.grid(row=0, column=1, padx=5, pady=5)
listbox_hourly_productions = tk.Listbox(
    listbox_frame, selectmode=tk.MULTIPLE, exportselection=0, height=8, relief="solid"
)
listbox_hourly_productions.grid(row=1, column=1, padx=5, pady=5, sticky="nsew")
listbox_hourly_productions.bind("<<ListboxSelect>>", update_selected_hourly_productions)

# Listbox for Event Types
label_event_types = ttk.Label(listbox_frame, text="Event Types", anchor="center")
label_event_types.grid(row=0, column=2, padx=5, pady=5)
listbox_event_types = tk.Listbox(
    listbox_frame, selectmode=tk.MULTIPLE, exportselection=0, height=8, relief="solid"
)
listbox_event_types.grid(row=1, column=2, padx=5, pady=5, sticky="nsew")
listbox_event_types.bind("<<ListboxSelect>>", update_selected_event_types)



# Frame for buttons
button_frame = ttk.LabelFrame(tab1, text="Plot Controls")
button_frame.pack(side=tk.TOP, padx=10, pady=5, fill="x")

# Layout for button controls
button_frame.columnconfigure((0, 1, 2, 3, 4), weight=1)  # Evenly distribute columns
quick_plot_button = ttk.Button(button_frame, text="Quick Plots", command=quick_plot)
quick_plot_button.grid(row=0, column=0, padx=5, pady=5, sticky="ew")

range_breaks_var = tk.BooleanVar()
range_breaks_checkbox = ttk.Checkbutton(button_frame, text="Ignore Time Gaps", variable=range_breaks_var)
range_breaks_checkbox.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

ttk.Label(button_frame, text="Tolerance").grid(row=0, column=2, padx=5, pady=5, sticky="e")
tolerance_entry = ttk.Entry(button_frame)
tolerance_entry.grid(row=0, column=3, padx=5, pady=5, sticky="ew")

calculate_slopes_var = tk.IntVar(value=0)
calculate_slopes_checkbox = ttk.Checkbutton(button_frame, text="Calculate Slopes", variable=calculate_slopes_var)
calculate_slopes_checkbox.grid(row=0, column=4, padx=5, pady=5, sticky="ew")

# Tab 2: New feature
tab2 = ttk.Frame(notebook)
notebook.add(tab2, text="Operative Info")

# Frame for BBOperative content
new_feature_frame = ttk.LabelFrame(tab2, text="Content of BBOperative")
new_feature_frame.pack(side=tk.TOP, padx=10, pady=10, fill="both", expand=True)

# Define grid structure for new_feature_frame
new_feature_frame.columnconfigure(0, weight=1)
new_feature_frame.columnconfigure(1, weight=3)  # Wider column for the textbox
new_feature_frame.rowconfigure((0, 1), weight=1)  # Allow rows to expand

# Frame for date and time selection
date_time_frame = ttk.Frame(new_feature_frame)
date_time_frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")

# Date filter section
date_filter_label = ttk.Label(date_time_frame, text="Select Date:")
date_filter_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")

date_combobox = ttk.Combobox(date_time_frame, values=[], state="readonly", width=20)
date_combobox.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
date_combobox.bind("<<ComboboxSelected>>", lambda event: update_time_listbox(event))

# Time filter section
time_selection_label = ttk.Label(date_time_frame, text="Select Time:")
time_selection_label.grid(row=1, column=0, padx=5, pady=5, sticky="w")

# Scrollbar for time listbox
ora_scrollbar = ttk.Scrollbar(date_time_frame, orient="vertical")

# Listbox for time selection
ora_listbox = tk.Listbox(
    date_time_frame, 
    selectmode=tk.MULTIPLE, 
    exportselection=0, 
    yscrollcommand=ora_scrollbar.set, 
    height=10,
    relief="solid"
)
ora_listbox.grid(row=1, column=1, padx=5, pady=5, sticky="nsew")
ora_scrollbar.grid(row=1, column=2, padx=(0, 5), pady=5, sticky="ns")
ora_scrollbar.config(command=ora_listbox.yview)

# Textbox for displaying selected data
selected_data_textbox = tk.Text(
    new_feature_frame, 
    wrap="word", 
    width=60, 
    relief="solid", 
    font=("Arial", 10)
)
selected_data_textbox.grid(row=0, column=1, rowspan=2, padx=10, pady=10, sticky="nsew")

# Button to open selected data in a browser
open_in_browser_button = ttk.Button(new_feature_frame, text="Open in Browser", command=open_selected_data_in_browser)
open_in_browser_button.grid(row=2, column=1, padx=10, pady=10, sticky="e")

# Directory label section
directory_label = ttk.Label(new_feature_frame, text="Directory:", font=("Arial", 10))
directory_label.grid(row=3, column=0, padx=10, pady=10, sticky="w")

# Bind listbox selection event to update selected data
ora_listbox.bind("<<ListboxSelect>>", update_selected_data)


# ---- Add the new tab for MySQL Query ----
tab3 = ttk.Frame(notebook)
notebook.add(tab3, text="MySQL Query")

# Main layout: Use a grid layout for better organization
tab3.columnconfigure(0, weight=1)
tab3.rowconfigure(0, weight=3)
tab3.rowconfigure(1, weight=1)
tab3.rowconfigure(2, weight=1)

# ---- Query Results Section ----
query_frame = ttk.LabelFrame(tab3, text="Query Results")
query_frame.grid(row=0, column=0, padx=10, pady=10, sticky="nsew")

# Add a Treeview widget to display query results
tree = ttk.Treeview(query_frame, columns=('Machine ID', 'State', 'Date and Time'), show='headings')
tree.heading('Machine ID', text='Machine ID')
tree.heading('State', text='State')
tree.heading('Date and Time', text='Date and Time')

# Set column widths
tree.column('Machine ID', width=100, anchor="center")
tree.column('State', width=100, anchor="center")
tree.column('Date and Time', width=200, anchor="center")

# Pack the Treeview widget
tree.pack(fill="both", expand=True)

# ---- Query Actions Section ----
button_sql_frame = ttk.LabelFrame(tab3, text="Query Actions")
button_sql_frame.grid(row=1, column=0, padx=10, pady=10, sticky="nsew")

# Distribute buttons evenly within the frame
button_sql_frame.columnconfigure((0, 1, 2, 3), weight=1)

# Create buttons for query actions
load_button = ttk.Button(button_sql_frame, text="Load Machines", command=load_machines)
load_button.grid(row=0, column=0, padx=5, pady=5, sticky="ew")

query_param_button = ttk.Button(button_sql_frame, text="Query Param2.txt", command=query_param2_txt)
query_param_button.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

query_calib_button = ttk.Button(button_sql_frame, text="Query Calib2.txt", command=query_calib2_txt)
query_calib_button.grid(row=0, column=2, padx=5, pady=5, sticky="ew")

query_log_button = ttk.Button(button_sql_frame, text="Query log.txt", command=query_log_txt)
query_log_button.grid(row=0, column=3, padx=5, pady=5, sticky="ew")

stop_button = ttk.Button(button_sql_frame, text="Stop Query", command=stop_status_check)
stop_button.grid(row=1, column=1, columnspan=2, padx=5, pady=5, sticky="ew")

# ---- Status Explanation Section ----
status_explanation_frame = ttk.LabelFrame(tab3, text="Status Explanation")
status_explanation_frame.grid(row=2, column=0, padx=10, pady=10, sticky="nsew")

status_explanation_label = ttk.Label(
    status_explanation_frame,
    text="Status Codes:\n"
         "1 - Command sent to the server\n"
         "2 - Attempting to send to the machine\n"
         "3 - Received by the machine\n"
         "4 - Sent by the machine"
)
status_explanation_label.pack(padx=10, pady=10)

# ---- Exit Button ----
exit_button = ttk.Button(root, text="Exit", command=exit_program)
exit_button.pack(side=tk.BOTTOM, pady=10)

# ---- Add the new tab for AI Analysis ----
tab4 = ttk.Frame(notebook)
notebook.add(tab4, text="AI Analysis")

# Main layout for AI Analysis tab
tab4.columnconfigure(0, weight=1)
tab4.rowconfigure(0, weight=1)

# Frame for AI Analysis
ai_frame = ttk.LabelFrame(tab4, text="AI Analysis")
ai_frame.pack(fill="both", expand=True, padx=10, pady=10)

# Frame for buttons
ai_button_frame = ttk.Frame(ai_frame)
ai_button_frame.pack(fill="x", padx=5, pady=5)

# --- FUNCIONES DE ANÁLISIS IA ---
def analyze_all_data():
    global data, bb_op_data, bb_eve_data, bb_alr_data
    
    try:
        # Verificar que los datos necesarios estén disponibles
        if data is None or data.empty:
            messagebox.showerror("Error", "No hay datos disponibles para analizar.")
            return
        
        # Verificar que las columnas necesarias existan
        required_columns = ['Date [dd/mm/yyyy]', 'Time [hh:mm:ss]', 'Event type']
        if not all(col in data.columns for col in required_columns):
            messagebox.showerror("Error", "Faltan columnas necesarias en los datos.")
            return
        
        # Crear columna Datetime si no existe
        if 'Datetime [yyyy-mm-dd hh:mm:ss.micro]' not in data.columns:
            data['Datetime'] = pd.to_datetime(
                data['Date [dd/mm/yyyy]'] + ' ' + data['Time [hh:mm:ss]'], 
                dayfirst=True
            )
            data = add_microseconds(data)
        
        # Preparar los datos para el análisis
        start_time = time.time()
        
        # Usar los últimos 20 registros si hay datos disponibles
        if not data.empty:
            analysis_data = data.tail(20).copy()  # Usar .copy() para evitar SettingWithCopyWarning
        else:
            analysis_data = pd.DataFrame()
        
        if not bb_op_data.empty:
            analysis_bb_op = bb_op_data.tail(20).copy()
        else:
            analysis_bb_op = pd.DataFrame()
            
        if not bb_eve_data.empty:
            analysis_bb_eve = bb_eve_data.tail(20).copy()
        else:
            analysis_bb_eve = pd.DataFrame()
            
        if not bb_alr_data.empty:
            analysis_bb_alr = bb_alr_data.tail(20).copy()
        else:
            analysis_bb_alr = pd.DataFrame()
        
        # Convertir todos los valores numéricos a tipos nativos de Python
        for df in [analysis_data, analysis_bb_op, analysis_bb_eve, analysis_bb_alr]:
            if not df.empty:
                for col in df.select_dtypes(include=['float64', 'int64']).columns:
                    df.loc[:, col] = df[col].apply(lambda x: float(x) if pd.notnull(x) else None)
        
        # Crear el analizador y realizar el análisis
        analyzer = OpenAIAnalyzer()
        
        # Preparar el prompt específico para el análisis
        prompt = """Analiza los siguientes datos y proporciona un resumen ejecutivo conciso:

1. Datos de Sensores:
- Identifica patrones en las lecturas de los sensores
- Analiza las tendencias y anomalías
- Evalúa la estabilidad de las mediciones

2. Datos de Producción:
- Evalúa la eficiencia de los ciclos de producción
- Identifica posibles cuellos de botella
- Analiza la consistencia de los resultados

3. Correlaciones:
- Identifica relaciones entre sensores y producción
- Evalúa el impacto de los eventos en el rendimiento
- Sugiere optimizaciones basadas en los datos

Proporciona recomendaciones específicas y accionables basadas en el análisis."""
        
        results = analyzer.analyze_all_data(
            analysis_data,
            analysis_bb_op,
            analysis_bb_eve,
            analysis_bb_alr,
            custom_prompt=prompt
        )
        
        end_time = time.time()
        logging.debug(f"Tiempo total de análisis: {end_time - start_time:.2f} segundos")
        
        # Mostrar los resultados
        if results:
            messagebox.showinfo("Análisis Completado", "El análisis se ha completado exitosamente.")
            print("\nResultados del análisis:")
            print(json.dumps(results, indent=2))
        else:
            messagebox.showwarning("Advertencia", "No se obtuvieron resultados del análisis.")
            
    except Exception as e:
        logging.error(f"Error durante el análisis: {str(e)}")
        messagebox.showerror("Error", f"Ocurrió un error durante el análisis: {str(e)}")

def analyze_specific_data():
    """Analiza un conjunto específico de datos."""
    if analyzer is None:
        messagebox.showerror("Error", "OpenAI no está disponible. Por favor, inicia el servicio OpenAI.")
        return
    try:
        start_time = time.time()
        logging.debug("Starting analyze_specific_data")
        
        selection_window = tk.Toplevel(root)
        selection_window.title("Seleccionar Datos")
        selection_window.geometry("300x200")
        selected_data = tk.StringVar(value="main")
        ttk.Radiobutton(selection_window, text="Datos Principales", variable=selected_data, value="main").pack(pady=5)
        ttk.Radiobutton(selection_window, text="Datos de Operación (BB_OP)", variable=selected_data, value="bb_op").pack(pady=5)
        ttk.Radiobutton(selection_window, text="Datos de Eventos (BB_EVE)", variable=selected_data, value="bb_eve").pack(pady=5)
        ttk.Radiobutton(selection_window, text="Datos de Alarmas (BB_ALR)", variable=selected_data, value="bb_alr").pack(pady=5)
        
        def analyze_selected():
            try:
                analysis_start = time.time()
                logging.debug(f"Starting analysis for {selected_data.get()}")
                
                data_type = selected_data.get()
                data_map = {
                    "main": data,
                    "bb_op": bb_op_data,
                    "bb_eve": bb_eve_data,
                    "bb_alr": bb_alr_data
                }
                
                analysis = analyzer.analyze_specific_data(data_type, data_map[data_type])
                logging.debug(f"AI analysis for {data_type} took {time.time() - analysis_start:.2f} seconds")
                
                ai_results_text.delete(1.0, tk.END)
                ai_results_text.insert(tk.END, analysis)
                
                logging.debug(f"Total analyze_selected execution took {time.time() - start_time:.2f} seconds")
                selection_window.destroy()
            except Exception as e:
                logging.error(f"Error in analyze_selected: {str(e)}", exc_info=True)
                messagebox.showerror("Error", f"Error al analizar datos: {str(e)}")
                
        ttk.Button(selection_window, text="Analizar", command=analyze_selected).pack(pady=10)
    except Exception as e:
        logging.error(f"Error in analyze_specific_data: {str(e)}", exc_info=True)
        messagebox.showerror("Error", f"Error al analizar datos: {str(e)}")

def analyze_correlations():
    """Analiza correlaciones entre los diferentes conjuntos de datos."""
    if analyzer is None:
        messagebox.showerror("Error", "OpenAI no está disponible. Por favor, inicia el servicio OpenAI.")
        return
    try:
        start_time = time.time()
        logging.debug("Starting analyze_correlations")
        
        analysis_start = time.time()
        analysis = analyzer.analyze_correlations(
            data,
            bb_op_data,
            bb_eve_data,
            bb_alr_data
        )
        logging.debug(f"AI analysis took {time.time() - analysis_start:.2f} seconds")
        
        ai_results_text.delete(1.0, tk.END)
        ai_results_text.insert(tk.END, analysis)
        
        logging.debug(f"Total analyze_correlations execution took {time.time() - start_time:.2f} seconds")
    except Exception as e:
        logging.error(f"Error in analyze_correlations: {str(e)}", exc_info=True)
        messagebox.showerror("Error", f"Error al analizar correlaciones: {str(e)}")

def analyze_sensors_and_production():
    """Analiza específicamente la relación entre datos de sensores y producción."""
    if analyzer is None:
        messagebox.showerror("Error", "OpenAI no está disponible. Por favor, inicia el servicio OpenAI.")
        return
    try:
        start_time = time.time()
        logging.debug("Starting analyze_sensors_and_production")
        
        analysis_start = time.time()
        analysis = analyzer.analyze_sensors_and_production(data, bb_op_data)
        logging.debug(f"AI analysis took {time.time() - analysis_start:.2f} seconds")
        
        ai_results_text.delete(1.0, tk.END)
        ai_results_text.insert(tk.END, analysis)
        
        logging.debug(f"Total analyze_sensors_and_production execution took {time.time() - start_time:.2f} seconds")
    except Exception as e:
        logging.error(f"Error in analyze_sensors_and_production: {str(e)}", exc_info=True)
        messagebox.showerror("Error", f"Error al analizar sensores y producción: {str(e)}")

# Button for analyzing all data
analyze_all_button = ttk.Button(
    ai_button_frame, 
    text="Analizar Todos los Datos",
    command=analyze_all_data
)
analyze_all_button.pack(side="left", padx=5)

# Button for analyzing specific data
analyze_specific_button = ttk.Button(
    ai_button_frame,
    text="Analizar Datos Específicos",
    command=analyze_specific_data
)
analyze_specific_button.pack(side="left", padx=5)

# Button for analyzing correlations
analyze_correlations_button = ttk.Button(
    ai_button_frame,
    text="Analizar Correlaciones",
    command=analyze_correlations
)
analyze_correlations_button.pack(side="left", padx=5)

# Button for analyzing sensors and production
analyze_sensors_button = ttk.Button(
    ai_button_frame,
    text="Analizar Sensores y Producción",
    command=analyze_sensors_and_production
)
analyze_sensors_button.pack(side="left", padx=5)

# Area for results with scrollbar
ai_results_frame = ttk.Frame(ai_frame)
ai_results_frame.pack(fill="both", expand=True, padx=5, pady=5)

ai_scrollbar = ttk.Scrollbar(ai_results_frame)
ai_scrollbar.pack(side="right", fill="y")

ai_results_text = tk.Text(
    ai_results_frame,
    height=20,
    width=80,
    yscrollcommand=ai_scrollbar.set,
    wrap="word"
)
ai_results_text.pack(side="left", fill="both", expand=True)
ai_scrollbar.config(command=ai_results_text.yview)

root.mainloop()
