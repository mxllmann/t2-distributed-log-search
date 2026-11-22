#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <glob.h>
#include <string.h>

typedef struct
{
    char file_path[256];
    int num_line;
    char line[1024];
} FoundOut;

typedef struct
{
    FoundOut *items;
    int count;
    int capacity;
} FoundList;

void add_found(FoundList *list, const char *file_path, int num_line, const char *line)
{
    if (list->count == list->capacity)
    {
        list->capacity = (list->capacity == 0) ? 16 : list->capacity * 2;
        list->items = realloc(list->items, list->capacity * sizeof(FoundOut));
    }

    FoundOut *item = &list->items[list->count];
    snprintf(item->file_path, sizeof(item->file_path), "%s", file_path);
    item->num_line = num_line;
    snprintf(item->line, sizeof(item->line), "%s", line);

    list->count += 1;
}

void search_word(const char *file_path, const char *word, FoundList *list)
{
    FILE *file = fopen(file_path, "r");
    if (file == NULL)
    {
        perror("Erro ao abrir arquivo");
        return;
    }

    char line[1024];
    int num_line = 0;

    while (fgets(line, sizeof(line), file) != NULL)
    {
        num_line += 1;

        if (strstr(line, word) != NULL)
        {
            add_found(list, file_path, num_line, line);
        }
    }

    fclose(file);
}

int count_files(const char *files_path, glob_t *results)
{
    char directories[512];
    snprintf(directories, sizeof(directories), "%s/*.log", files_path);

    int status = glob(directories, 0, NULL, results);

    if (status == 0)
    {
        return (int)results->gl_pathc;
    }

    if (status == GLOB_NOMATCH)
    {
        return 0;
    }

    return -1;
}

int main(int argc, char *argv[])
{
    MPI_Init(NULL, NULL);
    if (argc < 4)
    {
        printf("Please inform --dir <file_path> --search <search_word> --mode <static/dynamic>\n");
        return 1;
    }

    char *logs_path = NULL;
    char *word = NULL;
    char *mode = NULL;

    for (int i = 1; i < argc; i++)
    {
        if (strcmp(argv[i], "--dir") == 0 && i + 1 < argc)
        {
            logs_path = argv[++i];
        }
        else if (strcmp(argv[i], "--search") == 0 && i + 1 < argc)
        {
            word = argv[++i];
        }
        else if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc)
        {
            mode = argv[++i];
        }
    }

    int world_size;
    int world_rank;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    // To decide which process will take each file, I need to
    // make the file number % number of processes

    if (world_size < 2)
    {
        fprintf(stderr, "World size must be greather than 1 for %s\n", argv[0]);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (logs_path == NULL || word == NULL || mode == NULL)
    {
        if (world_rank == 0)
            printf("Uso: %s --dir <file_path> --search <search_word> --mode <static/dynamic>\n", argv[0]);
        MPI_Finalize();
        return 1;
    }

    glob_t results = {0};
    int num_files = count_files(logs_path, &results);

    if (world_rank == 0)
    {
        double start_time = MPI_Wtime();
        int workers = world_size - 1;
        fprintf(stderr, "Master printing. World rank: %i, logs path: %s\n", world_rank, logs_path);
        int processed = 0;
        int chunk_size = num_files / workers; // 3

        for (int i = 0; i < workers; i++)
        {
            int first_index = i * chunk_size;              // 0, 3, 6,
            int last_index = (processed + chunk_size) - 1; // 2, 5, 9,
            processed = last_index + 1;                    // 3, 6, 10

            if (i == workers - 1)
            {
                last_index = num_files - 1;
            }

            int blocks[2] = {first_index, last_index};
            MPI_Send(blocks, 2, MPI_INT, i + 1, 0, MPI_COMM_WORLD);
        }

        int *file_counts = calloc(num_files, sizeof(int));
        int total_occurrences = 0;

        printf("=== RESULTADOS DA BUSCA ===\n");

        for (int rank = 1; rank <= workers; rank++)
        {
            int count = 0;
            MPI_Recv(&count, 1, MPI_INT, rank, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            if (count > 0)
            {
                FoundOut *received = malloc(count * sizeof(FoundOut));
                MPI_Recv(received, count * sizeof(FoundOut), MPI_BYTE, rank, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                for (int j = 0; j < count; j++)
                {
                    printf("%s:%d: %s", received[j].file_path, received[j].num_line, received[j].line);
                    total_occurrences++;

                    for (int f = 0; f < num_files; f++)
                    {
                        if (strcmp(results.gl_pathv[f], received[j].file_path) == 0)
                        {
                            file_counts[f]++;
                            break;
                        }
                    }
                }

                free(received);
            }
        }

        double end_time = MPI_Wtime();

        printf("=== RESUMO ===\n");
        printf("Total de arquivos: %d\n", num_files);
        printf("Total de ocorrencias: %d\n", total_occurrences);

        for (int f = 0; f < num_files; f++)
        {
            printf("%s: %d ocorrencias\n", results.gl_pathv[f], file_counts[f]);
        }

        printf("Tempo de execucao: %.4f segundos\n", end_time - start_time);

        free(file_counts);
    }

    if (world_rank >= 1)
    {
        int blocks[2];
        MPI_Recv(blocks, 2, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        int first_index = blocks[0];
        int last_index = blocks[1];

        FoundList list = {NULL, 0, 0};

        for (int i = first_index; i <= last_index; i++)
        {
            search_word(results.gl_pathv[i], word, &list);
        }

        MPI_Send(&list.count, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);

        if (list.count > 0)
        {
            MPI_Send(list.items, list.count * sizeof(FoundOut), MPI_BYTE, 0, 2, MPI_COMM_WORLD);
        }

        free(list.items);
    }
    globfree(&results);
    MPI_Finalize();
    return 0;
}