trip.exe: trip.obj trip.mak
	link /codeview /nod /stack:16384 trip.obj,trip.exe,trip.map,slibca;

trip.obj: trip.c trip.mak
	cl /c /Zi /Fotrip.obj trip.c


