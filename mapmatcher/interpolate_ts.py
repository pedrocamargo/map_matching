def InterpolateTS(self, arrives_tstamps, leaves_tstamps, pathnodes, all_links):
    consistent = False
    while not consistent:
        consistent = True
        for i in range(len(pathnodes) - 1):
            o = pathnodes[i]
            if o in leaves_tstamps.keys():
                for j in range(i + 1, len(pathnodes)):
                    d = pathnodes[j]
                    if d in arrives_tstamps.keys():
                        if leaves_tstamps[o] > arrives_tstamps[d]:
                            if i > 0:
                                leaves_tstamps.pop(o, None)
                                arrives_tstamps.pop(o, None)
                            if d != pathnodes[-1]:
                                arrives_tstamps.pop(d, None)
                                leaves_tstamps.pop(d, None)
                            consistent = False
                            break
                if not consistent:
                    break
    i = 0
    while i < len(pathnodes) - 2:
        j = i + 1
        mp = self.network.interpolation_cost[all_links[j - 1]]
        while pathnodes[j] not in arrives_tstamps.keys():
            mp += self.network.interpolation_cost[all_links[j]]
            j += 1

        if j > i + 1:  # Means we have at least one node in the path that does not have any timestamp written to it
            time_diff = (arrives_tstamps[pathnodes[j]] - leaves_tstamps[pathnodes[i]]).total_seconds()
            if time_diff < 0:
                del arrives_tstamps
                del leaves_tstamps
                break
            mp2 = 0
            for k in range(i + 1, j):
                mp2 += self.network.interpolation_cost[all_links[k - 1]]
                j_time = leaves_tstamps[pathnodes[i]] + timedelta(seconds=time_diff * mp2 / mp)
                arrives_tstamps[pathnodes[k]] = j_time
                leaves_tstamps[pathnodes[k]] = j_time
        i = j
