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

void print_results(FoundList *all_found, glob_t *results, int num_files, double elapsed_time)
{
    int *file_counts = calloc(num_files, sizeof(int));

    printf("=== RESULTADOS DA BUSCA ===\n");
    for (int i = 0; i < all_found->count; i++)
    {
        FoundOut *item = &all_found->items[i];
        printf("%s:%d: %s", item->file_path, item->num_line, item->line);

        for (int f = 0; f < num_files; f++)
        {
            if (strcmp(results->gl_pathv[f], item->file_path) == 0)
            {
                file_counts[f]++;
                break;
            }
        }
    }

    printf("=== RESUMO ===\n");
    printf("Total de arquivos: %d\n", num_files);
    printf("Total de ocorrencias: %d\n", all_found->count);

    for (int f = 0; f < num_files; f++)
    {
        printf("%s: %d ocorrencias\n", results->gl_pathv[f], file_counts[f]);
    }

    printf("Tempo de execucao: %.4f segundos\n", elapsed_time);
    free(file_counts);
}

void run_static(int world_rank, int world_size, glob_t *results, int num_files, const char *word, FoundList *all_found)
{
    if (world_rank == 0)
    {
        int workers = world_size - 1;
        int processed = 0;
        int chunk_size = num_files / workers;

        for (int i = 0; i < workers; i++)
        {
            int first_index = i * chunk_size;
            int last_index = (processed + chunk_size) - 1;
            processed = last_index + 1;

            if (i == workers - 1)
            {
                last_index = num_files - 1;
            }

            int blocks[2] = {first_index, last_index};
            MPI_Send(blocks, 2, MPI_INT, i + 1, 0, MPI_COMM_WORLD);
        }

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
                    add_found(all_found, received[j].file_path, received[j].num_line, received[j].line);
                }

                free(received);
            }
        }
    }
    else
    {
        int blocks[2];
        MPI_Recv(blocks, 2, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        int first_index = blocks[0];
        int last_index = blocks[1];

        FoundList list = {NULL, 0, 0};

        for (int i = first_index; i <= last_index; i++)
        {
            search_word(results->gl_pathv[i], word, &list);
        }

        MPI_Send(&list.count, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);

        if (list.count > 0)
        {
            MPI_Send(list.items, list.count * sizeof(FoundOut), MPI_BYTE, 0, 2, MPI_COMM_WORLD);
        }

        free(list.items);
    }
}

void run_dynamic(int world_rank, int world_size, glob_t *results, int num_files, const char *word, FoundList *all_found)
{
    if (world_rank == 0)
    {
        int workers = world_size - 1;
        int next_file = 0;
        int active_workers = 0;
        FoundList *files_found = calloc(num_files, sizeof(FoundList));

        for (int rank = 1; rank <= workers; rank++)
        {
            int file_index = -1;

            if (next_file < num_files)
            {
                file_index = next_file;
                next_file += 1;
                active_workers += 1;
            }

            MPI_Send(&file_index, 1, MPI_INT, rank, 0, MPI_COMM_WORLD);
        }

        while (active_workers > 0)
        {
            int header[2];
            MPI_Status status;
            MPI_Recv(header, 2, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);

            int rank = status.MPI_SOURCE;
            int done_index = header[0];
            int count = header[1];

            if (count > 0)
            {
                FoundOut *received = malloc(count * sizeof(FoundOut));
                MPI_Recv(received, count * sizeof(FoundOut), MPI_BYTE, rank, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                files_found[done_index].items = received;
                files_found[done_index].count = count;
                files_found[done_index].capacity = count;
            }

            int file_index = -1;

            if (next_file < num_files)
            {
                file_index = next_file;
                next_file += 1;
            }
            else
            {
                active_workers -= 1;
            }

            MPI_Send(&file_index, 1, MPI_INT, rank, 0, MPI_COMM_WORLD);
        }

        for (int f = 0; f < num_files; f++)
        {
            for (int j = 0; j < files_found[f].count; j++)
            {
                FoundOut *item = &files_found[f].items[j];
                add_found(all_found, item->file_path, item->num_line, item->line);
            }

            free(files_found[f].items);
        }

        free(files_found);
    }
    else
    {
        while (1)
        {
            int file_index;
            MPI_Recv(&file_index, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            if (file_index < 0)
            {
                break;
            }

            FoundList list = {NULL, 0, 0};
            search_word(results->gl_pathv[file_index], word, &list);

            int header[2] = {file_index, list.count};
            MPI_Send(header, 2, MPI_INT, 0, 1, MPI_COMM_WORLD);

            if (list.count > 0)
            {
                MPI_Send(list.items, list.count * sizeof(FoundOut), MPI_BYTE, 0, 2, MPI_COMM_WORLD);
            }

            free(list.items);
        }
    }
}

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    int world_size;
    int world_rank;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

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

    if (world_size < 2)
    {
        if (world_rank == 0)
            fprintf(stderr, "World size must be greater than 1 for %s\n", argv[0]);
        MPI_Finalize();
        return 1;
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

    if (num_files <= 0)
    {
        if (world_rank == 0)
            fprintf(stderr, "Nenhum arquivo .log encontrado em %s\n", logs_path);
        globfree(&results);
        MPI_Finalize();
        return 1;
    }

    FoundList all_found = {NULL, 0, 0};

    MPI_Barrier(MPI_COMM_WORLD);
    double start_time = MPI_Wtime();

    if (strcmp(mode, "static") == 0)
    {
        run_static(world_rank, world_size, &results, num_files, word, &all_found);
    }
    else if (strcmp(mode, "dynamic") == 0)
    {
        run_dynamic(world_rank, world_size, &results, num_files, word, &all_found);
    }
    else
    {
        if (world_rank == 0)
        {
            fprintf(stderr, "Modo invalido: %s (use static ou dynamic)\n", mode);
        }
        globfree(&results);
        MPI_Finalize();
        return 1;
    }

    double end_time = MPI_Wtime();

    if (world_rank == 0)
    {
        print_results(&all_found, &results, num_files, end_time - start_time);
        free(all_found.items);
    }

    globfree(&results);
    MPI_Finalize();
    return 0;
}