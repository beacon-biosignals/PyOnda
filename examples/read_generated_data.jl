# Julia code (adapt paths): 
using Onda, Legolas, Arrow, TimeSpans, DataFrames, Dates, UUIDs
using Legolas: @schema, @version

# ---- Annotation table -----
# Will contain annotations events for all inference records
# Load table (not validated)
path_to_annotations = joinpath(pwd(), "data/table_test.annotations.arrow")
annotation_table = Arrow.Table(path_to_annotations)

# Construct schema compliant Table
path_to_annotations_validated = joinpath(pwd(), "data/table_test.annotations.validated.arrow")
annotations = [AnnotationV1(r) for r in Tables.rows(annotation_table)]
Legolas.write(path_to_annotations_validated, annotations, AnnotationV1SchemaVersion())

# Load table with Legolas.read
annotation_df = DataFrame(Legolas.read(path_to_annotations_validated))

# ---- Signal table -----
# Will contain SignalV2 rows for all records pointing to inference files
# Load signal table (not validated)
path_to_signal = joinpath(pwd(), "data/table_test.signal.arrow")
signal_table = Arrow.Table(path_to_signal)
signals = [SignalV2(r) for r in Tables.rows(signal_table)]

path_to_signals_validated = joinpath(pwd(), "data/table_test.signal.validated.arrow")
Legolas.write(path_to_signals_validated, signals, SignalV2SchemaVersion())

signal_df = DataFrame(Legolas.read(path_to_signals_validated))
Onda.load(signal_df[1,:]) 
