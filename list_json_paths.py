# -*- encoding: utf-8 -*-


def json_paths(json_object, print_paths=False, show_root_path=False, alpha_ordered=False, show_empty=True):
    """ Lists all json paths, author: Maximilian Erhard """
    def flatten(x):
        """ Remove lists of lists recursively """
        if isinstance(x, list):
            return [a for i in x for a in flatten(i)]
        else:
            return [x]

    def unique(sequence):
        """ Remove duplicates, keeps order """
        seen = set()
        return [x for x in sequence if not (x in seen or seen.add(x))]

    def lists_json_paths(json_object, path="", delimiter="$.", show_root_path=False, show_empty=True):
        """ Lists json paths, author: Maximilian Erhard """
        r = []
        if isinstance(json_object, dict):
            for key in json_object.keys():
                if isinstance(json_object[key], dict):
                    if show_root_path:
                        r.append([path + delimiter + key])
                    r.append(lists_json_paths(json_object[key], path + delimiter + key, delimiter=".",
                                              show_root_path=show_root_path,
                                              show_empty=show_empty))  # Mostra caminho do elemento
                elif isinstance(json_object[key], list):
                    if len(json_object[key]) > 0:
                        if show_root_path:
                            r.append([path + delimiter + key])
                        r.append(lists_json_paths(json_object[key], path + delimiter + key, delimiter="[].",
                                                  show_root_path=show_root_path,
                                                  show_empty=show_empty))  # Mostra caminho do elemento
                    else:
                        if show_empty:
                            r.append([path + delimiter + key + "[] <-- Vazio"])
                else:
                    r.append([path + delimiter + key])
        else:  # elif isinstance(json_dict, list):
            if len(json_object) > 0:
                # Pega a partir do primeiro elemento
                """
                if isinstance(json_object[0], dict):
                        r.append(lists_json_paths(json_object[0], path, delimiter="[].", show_root_path=show_root_path))  
                        # Mostra caminho do elemento
                    else:
                        r.append(["List Of List, in development ..."])
                """
                # Pega a partir de todos elementos
                for item in json_object:
                    if isinstance(item, dict):
                        r.append(lists_json_paths(item, path, delimiter="[].", show_root_path=show_root_path,
                                                  show_empty=show_empty))  # Mostra caminho do elemento
                    else:
                        r.append(["List Of List, in development ..."])
            else:
                r.append(["You've entered an Empty list."])
        return r

    json_paths = flatten(lists_json_paths(x, show_root_path=show_root_path, show_empty=show_empty))
    json_paths = list(sorted(set(json_paths))) if alpha_ordered else unique(json_paths)
    if print_paths:
        for path in json_paths:
            print(path)
    else:
        return json_paths
