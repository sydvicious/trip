#include <stdio.h>
#include <stdlib.h>

typedef struct {
	int	bestTotal;
	int	startDay;
	int	curTotal;
	int	numCity;
	int	totalCities;
	int	startCity;
	int	used[35];
	int	path[35];
	int sortedList[35];
} pathStruct;
/*
int max(int a, int b)
{
	if (a > b)
		return a;
	else
		return b;
}
*/
static pathStruct best, overall;
static int distance[35][35], wait[35][200], start, cities, days, startCity,
	maxTime, verbose;
static unsigned char cityNames[35][40];
static unsigned long traversals;
void outputResult(pathStruct thePath);
int sort(int from, pathStruct *attempt)
{
	int avail = 0, i, j, k, result;
	for (i = 0; i < cities; i++)
		if (attempt->used[i] == 0)
		{
			for (j = 0; j < avail; j++)
			{
				int	curDist, oldDist, curWait, oldWait;
				if (attempt->curTotal < 0)
				{
					curDist = distance[from][i];
					oldDist = distance[from][attempt->sortedList[j]];
					curWait = 0;
					oldWait = 0;
				}
				else
				{
					curDist = distance[from][i];
					oldDist = distance[from][attempt->sortedList[j]];
					curWait = wait[i][attempt->curTotal + curDist];
					oldWait = wait[attempt->sortedList[j]][attempt->curTotal + curDist];
				}
				if (curDist + curWait < oldDist + oldWait)
					break;
			}
			for (k = avail - 1; k >= j; k--)
				attempt->sortedList[k + 1] = attempt->sortedList[k];
			attempt->sortedList[j] = i;
			avail++;
		}
	return avail;
}
void traverse(int from, pathStruct attempt)
{
	int	i;

	traversals++;

	if (traversals == 0)
	{
		printf("traversals variable wrapped!\n");
		fflush(stdout);
	}
	else if (traversals % 10000000 == 0)
	{
		printf("traversals = %d.\n", traversals);
		fflush(stdout);
	}

	if (attempt.totalCities == cities)
	{
		attempt.curTotal += distance[from][attempt.startCity];
		if ((attempt.curTotal - attempt.startDay) < best.bestTotal)
			printf("NEW BEST PATH\n");
		else if (verbose)
			printf("PATH NOT BEST\n");
	 	if ((attempt.curTotal - attempt.startDay) < best.bestTotal)
		{
			best = attempt;
			best.bestTotal = attempt.curTotal - attempt.startDay;
			outputResult(best);
			printf("Traversal #%d\n", traversals);
			fflush(stdout);
	 	}
	}
	else
	{
		int	startTotal, numCities = sort(from, &attempt);
		
		startTotal = attempt.curTotal;
		attempt.totalCities++;
		for (i = 0; i < numCities; i++)
		{
			int	city = attempt.sortedList[i], j;
			if (verbose)
			{
				for (j = 0; j < attempt.totalCities - 1; j++)
					printf(" ");
				printf("%s\n", cityNames[city]);
			}
			attempt.curTotal = startTotal;
			if (attempt.used[city] == 0)
			{
				attempt.curTotal += distance[from][city];
	 			if (attempt.curTotal < 0)
				{
					if (verbose) printf("Before season starts. %d cities\n", attempt.totalCities);
					continue;
				}
				if (wait[city][attempt.curTotal] == 999)
				{
					if (verbose) printf("Team not coming home again. %d cities\n", attempt.totalCities);
					continue;
				}
	 			attempt.curTotal += wait[city][attempt.curTotal];
				if (attempt.curTotal - attempt.startDay >= best.bestTotal)
				{
					if (verbose) printf("Worse than best total already. %d cities\n", attempt.totalCities);
					continue;
				}
				if (attempt.curTotal >= days)
				{
					if (verbose) printf("After end of season. %d cities\n", attempt.totalCities);
					continue;
				}
				attempt.used[city]++;
				attempt.numCity++;
				attempt.path[attempt.numCity] = city;
	 			traverse(city, attempt);
				attempt.used[city] = 0;
				attempt.numCity--;
	 		}
			else if (verbose)
				printf("City already seen.\n");
		}
	} 
}
int getNextInteger(FILE *f)
{
	unsigned char	buffer[5];
	int		i;
	for (i = 0; i < 5; i++)
	{
		char	c;
		c = fgetc(f);
		if (c == '-' || (c >= '0' && c <= '9'))
			buffer[i] = c;
		else
		{
			buffer[i] = 0;
			break;
		}
	}
	return atoi(buffer);
}
void getCityName(FILE *f, unsigned char *string)
{
	unsigned char c;
	int	i = 0;
	while ((c = fgetc(f)) != '\n')
	{
		string[i] = c;
		i++;
	}
	string[i] = 0;
}
int initialize(FILE *f)
{
	int	i, j, maximum = 0;
	cities = getNextInteger(f);
	for (i = 0; i < cities; i++)
	{
		for (j = 0; j < cities; j++)
		{
			if (i == j)
			{
				distance[i][j] = 0;
				continue;
			}
			else
			{
				distance[i][j] = getNextInteger(f);
				maximum = max(maximum, distance[i][j]);
			}
		}
	}
	days = getNextInteger(f);
	for (i = 0; i < cities; i++)
		for (j = 0; j < days; j++)
			wait[i][j] = getNextInteger(f);
	for (i = 0; i < cities; i++)
		getCityName(f, cityNames[i]);
	return maximum;
}
void outputResult(pathStruct result)
{
	int	i, days;
	printf("Total days = %d\n", result.bestTotal);
	printf("Start city = %s\n", cityNames[result.startCity]);
	printf("Start day = %d\n", result.startDay);
	printf("%s\n", cityNames[result.startCity]);
	days = result.startDay;
	for (i = 1; i < cities; i++)
	{
		int	tripLength = distance[result.path[i - 1]][result.path[i]];
		int	thisWait = days + tripLength;
		printf("to %s, %d days on road, %d waiting in city\n",
			cityNames[result.path[i]], tripLength, wait[result.path[i]][thisWait]);
		days += tripLength + wait[result.path[i]][thisWait];
	}
	printf("to %s, %d days on road.\n", cityNames[result.startCity],
		distance[result.path[i - 1]][result.startCity]);
}
			
main(int argc, char *argv[])
{
	int	day, startDay;
	pathStruct	attempt;
	int	i, city;
	FILE	*f;

	traversals = 0;

	f = fopen("tables.tab", "r");
	startDay = initialize(f);
	startDay = -startDay + 1;
	maxTime = days - startDay;
	overall.bestTotal = maxTime;
	fclose(f);
	if (argc == 2 && argv[1][0] == '-' && argv[1][1] == 'v')
		verbose = 1;
	else
		verbose = 0;
	attempt.totalCities = 1;
	for (city = 0; city < cities; city++)
	{
		best.bestTotal = maxTime;
		attempt.startCity = city;
		for (i = 0; i < cities; i++)
		{
			if (i == city)
			{
				attempt.used[i] = 1;
			}
			else
			{
				attempt.used[i] = 0;
			}
		}
		for (day = 70; day > startDay; day--)
		{
			attempt.curTotal = day;
			attempt.startDay = day;
	 		attempt.path[0] = city;
			attempt.numCity = 0;
			printf("%s\n", cityNames[city]);
			traverse(city, attempt);
		}
		printf("\nBEST TOTAL FOR CITY %s:\n", cityNames[city]);
		outputResult(best);
		printf("\n");
		fflush(stdout);
		if (best.bestTotal < overall.bestTotal)
			overall = best;
	}
	printf("\n\nBEST OVERALL TOTAL\n");
	outputResult(overall);
}
