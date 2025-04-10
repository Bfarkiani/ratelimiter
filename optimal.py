import math
from docplex.mp.model import Model


def optimize(filename, B0, r, duration):
    """
    Optimize the rateLimiting MILP.

    Reads a request file of the form:
        userID  numRequests   comma-separated-arrivals
    Constructs and solves the rateLimiting problem:
        - Each request (i,j) must be accepted in a time slot t >= ceil(A[i][j]/0.1).
        - The objective minimizes the total delay: sum(0.1*x[i,j] - A[i][j]).

    Args:
        filename: Name of the file with requests.
        B0: Token bucket capacity (also the initial token count).
        r: Token generation rate per time slot.
        duration: Number of time slots (each slot is 0.1 seconds).

    Returns:
        m: The model.
        solution: The solution obtained from solving the model.
    Please note that you need CPLEX installation. 
    """
    # Read request file, storing user IDs and arrival times.
    I = []
    A = {}
    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            i_user = int(parts[0])
            arrivals = [float(x) for x in parts[2].split(',')]
            I.append(i_user)
            A[i_user] = arrivals

    # Define request index set for each user.
    J = {i: range(len(A[i])) for i in I}
    T = range(1, duration + 1)  # Each slot = 0.1 seconds

    m = Model(name="rateLimiting")

    # Define binary assignment variables: z[i,j,t] indicates request (i,j) is accepted at slot t.
    z_vars = {}
    for i in I:
        for j in J[i]:
            t_min = math.ceil(A[i][j] / 0.1)
            if t_min <= duration:
                for t in range(t_min, duration + 1):
                    z_vars[(i, j, t)] = m.binary_var(name=f"z_{i}_{j}_{t}")

    # Define acceptance time variable x[i,j] (integer variable in [1, duration])
    x = {}
    for i in I:
        for j in J[i]:
            x[i, j] = m.integer_var(lb=1, ub=duration, name=f"x_{i}_{j}")

    # Token bucket state variables y[t] (continuous, in [0, B0])
    y_vars = {0: B0}
    for t in range(1, duration + 1):
        y_vars[t] = m.continuous_var(lb=0, ub=B0, name=f"y_{t}")
    # Binary helper variables delta[t]
    delta = {t: m.binary_var(name=f"delta_{t}") for t in range(1, duration + 1)}

    # Constraint: Each request must be accepted exactly once.
    for i in I:
        for j in J[i]:
            t_min = math.ceil(A[i][j] / 0.1)
            feasible_t = [t for t in range(t_min, duration + 1) if (i, j, t) in z_vars]
            if feasible_t:
                m.add_constraint(m.sum(z_vars[(i, j, t)] for t in feasible_t) == 1, ctname=f"one_accept_{i}_{j}")

    # Constraint: FIFO ordering for requests from the same user.
    for i in I:
        num_reqs = len(J[i])
        for j_idx in range(num_reqs - 1):
            j_cur = j_idx
            j_nxt = j_idx + 1
            t_min_nxt = math.ceil(A[i][j_nxt] / 0.1)
            for t in T:
                if t < t_min_nxt:
                    continue
                lhs_nxt = [z_vars[(i, j_nxt, tau)] for tau in range(math.ceil(A[i][j_nxt] / 0.1), min(t, duration) + 1)
                           if (i, j_nxt, tau) in z_vars]
                lhs_cur = [z_vars[(i, j_cur, tau)] for tau in range(math.ceil(A[i][j_cur] / 0.1), min(t, duration) + 1)
                           if (i, j_cur, tau) in z_vars]
                m.add_constraint(m.sum(lhs_nxt) <= m.sum(lhs_cur), ctname=f"order_{i}_{j_cur}_{j_nxt}_t{t}")

    # Define Z[t]: number of requests accepted at slot t.
    Z = {t: m.sum(z_vars[(i, j, t)] for (i, j, t_val) in z_vars if t_val == t) for t in T}
    M = B0 + r * duration  # Big-M constant

    # Token bucket dynamics constraints for each time slot.
    for t in T:
        m.add_constraint(y_vars[t] <= y_vars[t - 1] - Z[t] + r, ctname=f"tb_upper1_t{t}")
        m.add_constraint(y_vars[t] <= B0, ctname=f"tb_upper2_t{t}")
        m.add_constraint(y_vars[t] >= y_vars[t - 1] - Z[t] + r - M * (1 - delta[t]), ctname=f"tb_lower1_t{t}")
        m.add_constraint(y_vars[t] >= B0 - M * delta[t], ctname=f"tb_lower2_t{t}")

    # Define x[i,j] to be the actual acceptance slot for request (i,j).
    for i, j in x:
        t_min = math.ceil(A[i][j] / 0.1)
        feasible_t = [t for t in range(t_min, duration + 1) if (i, j, t) in z_vars]
        m.add_constraint(x[i, j] == m.sum(t * z_vars[(i, j, t)] for t in feasible_t), ctname=f"def_x_{i}_{j}")

    # Objective: minimize total delay.
    m.minimize(m.sum(0.1 * x[i, j] - A[i][j] for i in I for j in J[i]))

    # Export and dump the LP formulation.
    lp_formulation = m.export_as_lp()
    print("LP formulation of the problem:")
    print(lp_formulation)
    m.dump_as_lp("MILPproblem.lp")

    solution = m.solve(log_output=True)

    if solution is not None:
        print(f"Optimal objective (total delay) = {solution.get_objective_value():.3f}")
        with open("solution.txt", "w") as f_out:
            for i in I:
                for j in J[i]:
                    accept_time = solution.get_value(x[i, j])
                    arrival = A[i][j]
                    f_out.write(
                        f"User {i}, Req {j}: accepted_slot={accept_time}, actual_time={0.1 * accept_time}, arrival={arrival}\n")
        with open("solution_y.txt", "w") as f_y:
            for t in range(0, duration + 1):
                y_val = y_vars[0] if t == 0 else solution.get_value(y_vars[t])
                f_y.write(f"Slot {t}: y[{t}] = {y_val}\n")
    else:
        print("No solution found.")

    return m, solution


# Run the optimization with specified parameters.
model, sol = optimize(
    filename='requests.txt',
    B0=2,  # Token bucket capacity (initial tokens)
    r=0.05,  # Token generation rate per time slot
    duration=120  # 120 time slots (each slot = 0.1 seconds)
)
